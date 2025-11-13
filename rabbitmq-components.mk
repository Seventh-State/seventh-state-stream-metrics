# Set default goal to 'all' if not already set
ifeq ($(.DEFAULT_GOAL),)
.DEFAULT_GOAL = all
endif

# Set PROJECT_VERSION if not provided
ifeq ($(PROJECT_VERSION),)
PROJECT_VERSION := $(shell \
    if test -f git-revisions.txt; then \
        head -n1 git-revisions.txt | \
        awk '{print $$$(words $(PROJECT_DESCRIPTION) version);}'; \
    else \
        (git describe --dirty --abbrev=7 --tags --always --first-parent 2>/dev/null || echo rabbitmq_v0_0_0) | \
        sed -e 's/^rabbitmq_v//' -e 's/^v//' -e 's/_/./g' -e 's/-/+/' -e 's/-/./g'; \
    fi)
endif

# Detect and export current RabbitMQ reference
ifeq ($(origin current_rmq_ref),undefined)
  ifneq ($(wildcard .git),)
    current_rmq_ref := $(shell (\
      ref=$$(LANG=C git branch --list | awk '/^\* \(.*detached / {ref=$$0; sub(/.*detached [^ ]+ /, "", ref); sub(/\)$$/, "", ref); print ref; exit;} /^\* / {ref=$$0; sub(/^\* /, "", ref); print ref; exit}');\
      if test "$$(git rev-parse --short HEAD)" != "$$ref"; then echo "$$ref"; fi))
  else
    current_rmq_ref := main
  endif
endif
export current_rmq_ref

# Detect and export base RabbitMQ reference
ifeq ($(origin base_rmq_ref),undefined)
  ifneq ($(wildcard .git),)
    possible_base_rmq_ref := main
    ifeq ($(possible_base_rmq_ref),$(current_rmq_ref))
      base_rmq_ref := $(current_rmq_ref)
    else
      base_rmq_ref := $(shell \
        (git rev-parse --verify -q main >/dev/null && \
         git rev-parse --verify -q $(possible_base_rmq_ref) >/dev/null && \
         git merge-base --is-ancestor $$(git merge-base main HEAD) $(possible_base_rmq_ref) && \
         echo $(possible_base_rmq_ref)) || \
        echo main)
    endif
  else
    base_rmq_ref := main
  endif
endif
export base_rmq_ref

RABBITMQ_REPO ?= https://github.com/rabbitmq/rabbitmq-server.git

# Built-in RabbitMQ components
RABBITMQ_BUILTIN = \
    amqp10_client \
    amqp10_common \
    amqp_client \
    oauth2_client \
    rabbit \
    rabbit_common \
    rabbitmq_amqp1_0 \
    rabbitmq_amqp_client \
    rabbitmq_auth_backend_cache \
    rabbitmq_auth_backend_http \
    rabbitmq_auth_backend_internal_loopback \
    rabbitmq_auth_backend_ldap \
    rabbitmq_auth_backend_oauth2 \
    rabbitmq_auth_mechanism_ssl \
    rabbitmq_aws \
    rabbitmq_cli \
    rabbitmq_codegen \
    rabbitmq_consistent_hash_exchange \
    rabbitmq_ct_client_helpers \
    rabbitmq_ct_helpers \
    rabbitmq_event_exchange \
    rabbitmq_federation \
    rabbitmq_federation_management \
    rabbitmq_federation_prometheus \
    rabbitmq_jms_topic_exchange \
    rabbitmq_management \
    rabbitmq_management_agent \
    rabbitmq_mqtt \
    rabbitmq_peer_discovery_aws \
    rabbitmq_peer_discovery_common \
    rabbitmq_peer_discovery_consul \
    rabbitmq_peer_discovery_etcd \
    rabbitmq_peer_discovery_k8s \
    rabbitmq_prelaunch \
    rabbitmq_prometheus \
    rabbitmq_random_exchange \
    rabbitmq_recent_history_exchange \
    rabbitmq_sharding \
    rabbitmq_shovel \
    rabbitmq_shovel_management \
    rabbitmq_stomp \
    rabbitmq_stream \
    rabbitmq_stream_common \
    rabbitmq_stream_management \
    rabbitmq_top \
    rabbitmq_tracing \
    rabbitmq_trust_store \
    rabbitmq_web_dispatch \
    rabbitmq_web_mqtt \
    rabbitmq_web_mqtt_examples \
    rabbitmq_web_stomp \
    rabbitmq_web_stomp_examples \
    trust_store_http

# Define fetch rules for built-in components
$(foreach dep,$(RABBITMQ_BUILTIN), \
    $(eval dep_$(dep) = git_c $(RABBITMQ_REPO) $(current_rmq_ref) $(dep)) \
)

# Fetch rule for git_c dependencies
define dep_fetch_git_c
    if [ ! -d $(DEPS_DIR)/rabbitmq_server/.git ]; then \
        git clone -q --branch $(call query_version_git,$1) --single-branch $(call query_repo_git,$1) $(DEPS_DIR)/rabbitmq_server; \
    fi; \
    ln -sfn $(DEPS_DIR)/rabbitmq_server/deps/$(word 4,$(dep_$1)) $(DEPS_DIR)/$(call query_name,$1); \
    if [ "$(call query_name,$1)" = "rabbitmq_cli" ]; then \
        if [ -f patches/rabbitmq_cli.patch ]; then \
            echo " PATCH\trabbitmq_cli (workaround for RabbitMQ 4.1+)"; \
            cd $(DEPS_DIR)/rabbitmq_cli && patch -p1 --verbose < ../../../../patches/rabbitmq_cli.patch || true; \
        fi; \
    fi
endef

# Community RabbitMQ components
RABBITMQ_COMMUNITY = \
    rabbitmq_auth_backend_amqp \
    rabbitmq_boot_steps_visualiser \
    rabbitmq_delayed_message_exchange \
    rabbitmq_lvc_exchange \
    rabbitmq_management_exchange \
    rabbitmq_management_themes \
    rabbitmq_message_timestamp \
    rabbitmq_metronome \
    rabbitmq_routing_node_stamp \
    rabbitmq_rtopic_exchange

# Define fetch rules with kebab-case repo names
$(foreach dep,$(RABBITMQ_COMMUNITY), \
  $(eval dep_$(dep) = git https://github.com/rabbitmq/$(subst _,-,$(dep)).git $(current_rmq_ref)) \
)

RABBITMQ_COMPONENTS = $(RABBITMQ_BUILTIN) $(RABBITMQ_COMMUNITY)
FORCE_REBUILD += $(RABBITMQ_COMPONENTS)
NO_AUTOPATCH += $(RABBITMQ_COMPONENTS)

# Expand dependencies based on DEPS
ifneq (,$(findstring rabbit,$(DEPS)))
    BUILD_DEPS += rabbit_common rabbitmq_codegen rabbitmq_cli amqp10_common

    # Workaround to build with RabbitMQ 3.13+
    ifneq ($(shell \
      [ "$$(printf "%s\nv3.13\n$(current_rmq_ref)" | sort -V | tail -n1)" = "$(current_rmq_ref)" ] && echo ok),)
        BUILD_DEPS += rabbitmq_prelaunch
    endif
endif

ifneq (,$(findstring rabbitmq_management,$(DEPS)))
    DEPS += amqp_client rabbitmq_web_dispatch rabbitmq_management_agent

    # Workaround to build with RabbitMQ 3.13+
    ifneq ($(shell \
      [ "$$(printf "%s\nv3.13\n$(current_rmq_ref)" | sort -V | tail -n1)" = "$(current_rmq_ref)" ] && echo ok),)
        DEPS += oauth2_client
    endif
endif

# Add seshat as a build dependency for RabbitMQ 4+ workaround
BUILD_DEPS += seshat
ifneq ($(shell \
  [ "$$(printf "%s\nv4.2\n$(current_rmq_ref)" | sort -V | tail -n1)" = "$(current_rmq_ref)" ] && echo ok),)
    dep_seshat = hex 1.0.0
else
    dep_seshat = git https://github.com/rabbitmq/seshat v0.6.1
endif
