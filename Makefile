PROJECT             = seventh_state_stream_metrics
PROJECT_DESCRIPTION = Seventh State Stream Metrics Plugin
PROJECT_MOD         = seven_stream_metrics_app
PROJECT_VERSION     = 1.0.0

RABBITMQ_VERSION ?= v4.2.x
current_rmq_ref ?=$(RABBITMQ_VERSION)

define PROJECT_APP_EXTRA_KEYS
    {broker_version_requirements, []}
endef


# dep_amqp_client                = git_rmq-subfolder rabbitmq-erlang-client $(current_rmq_ref)
# dep_rabbit_common              = git_rmq-subfolder rabbitmq-common $(current_rmq_ref)
# dep_rabbit                     = git_rmq-subfolder rabbitmq-server $(current_rmq_ref)
# dep_rabbitmq_ct_client_helpers = git_rmq-subfolder rabbitmq-ct-client-helpers $(current_rmq_ref)
# dep_rabbitmq_ct_helpers        = git_rmq-subfolder rabbitmq-ct-helpers $(current_rmq_ref)
# dep_rabbit_stream_common    = git_rmq-subfolder rabbitmq-stream-common $(current_rmq_ref)
# dep_rabbitmq_stream_common = git_rmq-subfolder rabbitmq-stream-common $(current_rmq_ref)

DEPS      = rabbit rabbit_common
TEST_DEPS = amqp_client# rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS       = rabbit_common/mk/rabbitmq-plugin.mk

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# Set log directory to be under the current project root directory
# Otherwise, erlang.mk will set it to a weird path
CT_LOGS_DIR = $(CURDIR)/logs
