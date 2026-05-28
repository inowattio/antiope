#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="kafka:9092"

USER_FILE="/etc/kafka/provision/user.json"
TOPIC_FILE="/etc/kafka/provision/topic.json"
ACL_FILE="/etc/kafka/provision/acl.json"
STATE_FILE="/etc/kafka/provision/.provision-state.json"

DEFAULT_PARTITIONS=${DEFAULT_PARTITIONS}
DEFAULT_REPLICATION_FACTOR=${DEFAULT_REPLICATION_FACTOR}
SCRAM_MECHANISM="${SCRAM_MECHANISM}"

ADMIN_USERNAME="admin"
ADMIN_PASSWORD="admin"

######################################################################################
#CREDENTIALS SETUP 
######################################################################################

echo "Creating creds config file"

setup_credentials() {
  TMPFILE=$(mktemp)

  cat <<EOT > "$TMPFILE"
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${ADMIN_USERNAME}" password="${ADMIN_PASSWORD}";
EOT
}

cleanup() {
  rm -f "$TMPFILE"
}

######################################################################################
#STATE
######################################################################################

init_state() {
  if [ ! -f "$STATE_FILE" ]; then
    echo '{}' > "$STATE_FILE"
    chmod 644 "$STATE_FILE"
  fi
}

file_hash() {
  sha256sum "$1" | awk '{print $1}'
}

get_saved_hash() {
  local key="$1"

  jq -r --arg key "$key" '.[$key] // empty' "$STATE_FILE"
}

save_hash() {
  local key="$1"
  local hash="$2"
  local tmp

  tmp=$(mktemp)

  jq --arg key "$key" --arg hash "$hash" \
    '.[$key] = $hash' "$STATE_FILE" > "$tmp"

  mv "$tmp" "$STATE_FILE"
  chmod 644 "$STATE_FILE"
}

should_run() {
  local key="$1"
  local file="$2"

  local current_hash
  local saved_hash

  current_hash=$(file_hash "$file")
  saved_hash=$(get_saved_hash "$key")

  [ "$current_hash" != "$saved_hash" ]
}

######################################################################################
#USERS
######################################################################################

provision_users() {
  echo "Creating users..."

  local user_count
  user_count=$(jq '.users | length' "$USER_FILE")

  for i in $(seq 0 $((user_count - 1))); do
    local user
    local username
    local password

    user=$(jq ".users[$i]" "$USER_FILE")
    username=$(echo "$user" | jq -r ".username")
    password=$(echo "$user" | jq -r ".password")

    kafka-configs --bootstrap-server "$BOOTSTRAP" \
      --alter \
      --command-config "$TMPFILE" \
      --add-config "${SCRAM_MECHANISM}=[password=${password}]" \
      --entity-type users \
      --entity-name "$username"

    echo "User created/updated: $username"
  done
}

######################################################################################
#TOPICS
######################################################################################

provision_topics() {
  echo "Creating topics..."

  local topic_count
  topic_count=$(jq '.topics | length' "$TOPIC_FILE")

  for i in $(seq 0 $((topic_count - 1))); do
    local topic
    local name
    local partitions
    local replication_factor
    local config_count

    topic=$(jq ".topics[$i]" "$TOPIC_FILE")
    name=$(echo "$topic" | jq -r ".name")

    partitions=$(echo "$topic" | jq -r --arg d "$DEFAULT_PARTITIONS" '
      .partition_number // $d
    ')

    replication_factor=$(echo "$topic" | jq -r --arg d "$DEFAULT_REPLICATION_FACTOR" '
      .replication_factor // $d
    ')

    kafka-topics --bootstrap-server "$BOOTSTRAP" \
      --create \
      --command-config "$TMPFILE" \
      --if-not-exists \
      --topic "$name" \
      --partitions "$partitions" \
      --replication-factor "$replication_factor"

    config_count=$(echo "$topic" | jq ".config // {} | length")

    if [ "$config_count" -gt 0 ]; then
      local configs

      configs=$(echo "$topic" | jq -r '
        .config
        | to_entries
        | map("\(.key)=\(.value)")
        | join(",")
      ')

      kafka-configs --bootstrap-server "$BOOTSTRAP" \
        --alter \
        --command-config "$TMPFILE" \
        --entity-type topics \
        --entity-name "$name" \
        --add-config "$configs"
    fi

    echo "Topic created/updated: $name"
  done
}

######################################################################################
#ACLS
######################################################################################

make_operation_args() {
  local ops
  local operation

  mapfile -t ops < <(echo "$OPERATIONS" | jq -r '.[]')

  OPERATION_ARGS=()

  for operation in "${ops[@]}"; do
    OPERATION_ARGS+=(--operation "$operation")
  done
}

make_resource_args() {
  declare -A resource_map=(
    [Topic]="topic"
    [Group]="group"
    [Cluster]="cluster"
    [TransactionalId]="transactional-id"
    [DelegationToken]="delegation-token"
  )

  local flag
  flag="${resource_map[$RESOURCE_TYPE]:-}"

  if [[ -z "$flag" ]]; then
    echo "Unsupported resource type: $RESOURCE_TYPE"
    exit 1
  fi

  RESOURCE_ARGS=()

  if [[ "$RESOURCE_TYPE" == "Cluster" ]]; then
    RESOURCE_ARGS=(--"$flag")
  else
    RESOURCE_ARGS=(--"$flag" "$RESOURCE_NAME")
  fi
}

make_principal_args() {
  declare -A permission_map=(
    [allow]="allow"
    [deny]="deny"
  )

  local permission
  permission="${permission_map[$PERMISSION_TYPE]:-}"

  if [[ -z "$permission" ]]; then
    echo "Unsupported permission type: $PERMISSION_TYPE"
    exit 1
  fi

  PRINCIPAL_ARGS=(
    --"${permission}-principal" "User:$USERNAME"
    --"${permission}-host" "$TARGET_HOST"
  )
}

provision_acls() {
  echo "Creating ACLs..."

  local users
  users=$(jq -r '.acls | keys[]' "$ACL_FILE")

  for USERNAME in $users; do
    local acl_count
    acl_count=$(jq ".acls[\"$USERNAME\"] | length" "$ACL_FILE")

    for i in $(seq 0 $((acl_count - 1))); do
      local rule

      rule=$(jq -c ".acls[\"$USERNAME\"][$i]" "$ACL_FILE")

      RESOURCE_TYPE=$(echo "$rule" | jq -r '.resource_type')
      RESOURCE_NAME=$(echo "$rule" | jq -r '.resource_name // empty')
      PATTERN_TYPE=$(echo "$rule" | jq -r '.pattern_type // "literal"')
      TARGET_HOST=$(echo "$rule" | jq -r '.target_host // "*"')
      PERMISSION_TYPE=$(echo "$rule" | jq -r '.permission_type // "allow"')
      OPERATIONS=$(echo "$rule" | jq -c '.operations')

      make_operation_args
      make_resource_args
      make_principal_args

      kafka-acls \
        --command-config "$TMPFILE" \
        --bootstrap-server "$BOOTSTRAP" \
        --add \
        "${PRINCIPAL_ARGS[@]}" \
        "${OPERATION_ARGS[@]}" \
        --resource-pattern-type "$PATTERN_TYPE" \
        "${RESOURCE_ARGS[@]}" \
        || true

      echo "ACL applied: User:$USERNAME $RESOURCE_TYPE:$RESOURCE_NAME $PATTERN_TYPE"
    done
  done
}

run_if_changed() {
  local key="$1"
  local file="$2"
  local func="$3"

  if should_run "$key" "$file"; then
    echo "$key changed. Running provisioning..."

    "$func"

    save_hash "$key" "$(file_hash "$file")"

    echo "$key hash saved."
  else
    echo "$key unchanged. Skipping."
  fi
}

######################################################################################
#MAIN
######################################################################################

microdnf install -y jq

setup_credentials
trap cleanup EXIT
init_state

run_if_changed "users" "$USER_FILE" provision_users
run_if_changed "topics" "$TOPIC_FILE" provision_topics
run_if_changed "acls" "$ACL_FILE" provision_acls

echo "Kafka seed completed."
