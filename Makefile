PROJECT             = seventh_state_stream_metrics
PROJECT_DESCRIPTION = Seventh State Stream Metrics Plugin
PROJECT_MOD         = seven_stream_metrics_app
PROJECT_VERSION     = 1.0.0

RABBITMQ_VERSION ?= v4.2.x
current_rmq_ref ?=$(RABBITMQ_VERSION)

define PROJECT_APP_EXTRA_KEYS
    {broker_version_requirements, []}
endef


dep_rabbitmq_ct_client_helpers = git_rmq-subfolder rabbitmq-ct-client-helpers $(current_rmq_ref)
dep_rabbitmq_stream_common = git_rmq-subfolder rabbitmq-stream-common $(current_rmq_ref)
dep_rabbitmq_management = git_rmq-subfolder rabbitmq-management $(current_rmq_ref)

DEPS      = rabbit rabbit_common rabbitmq_prometheus rabbitmq_management oauth2_client
TEST_DEPS = amqp_client rabbitmq_ct_helpers rabbitmq_ct_client_helpers rabbitmq_stream_common rabbitmq_stream

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS       = rabbit_common/mk/rabbitmq-plugin.mk

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# Set log directory to be under the current project root directory
# Otherwise, erlang.mk will set it to a weird path
CT_LOGS_DIR = $(CURDIR)/logs


# Ensure framework is present
fw:
	@if [ ! -d "src/extension-framework" ]; then \
		if [ -n "$${GITHUB_TOKEN}" ]; then \
			CLEAN_TOKEN=$$(echo "$${GITHUB_TOKEN}" | tr -d '[:space:]'); \
			git clone --depth 1 "https://x-access-token:$${CLEAN_TOKEN}@github.com/Seventh-State/Seventh-State-RabbitMQ-Extension-Framework.git" src/extension-framework; \
		else \
			git clone --depth 1 git@github.com:Seventh-State/Seventh-State-RabbitMQ-Extension-Framework.git src/extension-framework; \
		fi; \
	fi

MANIFEST = package/manifest.yaml

package: fw
	@echo "Building packages using Seventh-State framework..."
	rm -rf $(PWD)/dist/*
	$(MAKE) -C src/extension-framework build-linux build-docker build-ez MANIFEST=$(PWD)/$(MANIFEST) OUTPUT_DIR=$(PWD)/dist
