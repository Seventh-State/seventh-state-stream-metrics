# Seventh State Stream Metrics Plugin

This repository is a **template plugin** for Seventh State plugins. You can use it as a starting point for your own plugin development.

## Getting Started

You can choose to build and test the plugin using either:

* **Docker Compose** (to avoid local environment issues), or
* **Your local setup** (if you already have compatible Erlang/Elixir installed).

## Build and Test Locally (Without Docker)

If you prefer to work locally, make sure you have:

* **Erlang/OTP 26.2**
* **Elixir 1.14.5**

These versions are compatible with most RabbitMQ versions: from `3.12.10` to `4.x`.

No need to install RabbitMQ — this project can start a broker for you.

## Common Makefile Commands

```bash
gmake tests                          # Run all tests (logs appear in ./logs)
gmake run-broker                     # Start RabbitMQ broker with an interactive Erlang shell
gmake start-cluster                  # Start a 3-node RabbitMQ cluster (customise with NODES=5)
gmake stop-cluster                   # Stop the running cluster
gmake dist DIST_AS_EZS=1             # Create a .ez plugin file (output in ./plugins)
gmake ct-a_test_suite                # Run a test suite
gmake ct-a_test_suite t="group"      # Run a test group inside the suite
gmake ct-a_test_suite t="group:case" # Run a specific test case
```

> Test files are located in the `test/` directory.
> If you add new plugin functionality, you should add corresponding tests there.

> To install and test your `.ez` plugin on any RabbitMQ node, follow the official plugin installation guide:
> [https://www.rabbitmq.com/plugins.html](https://www.rabbitmq.com/plugins.html)

> For more useful commands and development guidelines, refer to the official RabbitMQ contribution guide:
> [https://github.com/rabbitmq/rabbitmq-server/blob/main/CONTRIBUTING.md](https://github.com/rabbitmq/rabbitmq-server/blob/main/CONTRIBUTING.md)

## Build and Test with Docker Compose

1. **Build the Docker image:**

   ```bash
   docker compose -f build/docker-compose.yml build --no-cache
   ```

2. **Run tests and build the plugin:**

   ```bash
   docker compose -f build/docker-compose.yml run --rm test-and-build make tests
   docker compose -f build/docker-compose.yml run --rm test-and-build make dist DIST_AS_EZS=1
   ```

   > If you see a `flock`-related error like:
   > `flock: can't open 'sbin.lock': No such file or directory`
   > just re-run the command — it’s usually transient.

3. **Test logs and build artifacts** will be available in your project root directory after the run.

   * `logs/` – contains test logs.
   * `plugins/` – contains the generated `.ez` plugin files.

> This approach avoids local dependency/version issues.

## Template for Plugin with Custom RabbitMQ

This repository also provides a template for developing plugins with a **custom RabbitMQ build**.
If your plugin requires changes to the RabbitMQ source code (e.g. patching core modules), you should use the `stream-metrics-with-custom-rabbit` branch as a starting point.
It includes everything needed to build, test, and release your plugin alongside a custom RabbitMQ version.

### Step 1: Build RabbitMQ (Custom Branch)

1. Clone the private RabbitMQ repository:

   ```bash
   git clone https://github.com/Seventh-State/rabbitmq-server-private.git
   ```
2. Create a new custom branch using the format: `<version>-<plugin-name>`

   Example:

   ```
   v4.1.2-hello-plugin
   ```
3. Make your changes and push the branch to GitHub.
4. Navigate to the **GitHub Actions** page of the plugin repository.
5. **Trigger the Build RabbitMQ server** for your branch by selecting it from the workflow UI.


### Step 2: Generate Manifest File

1. After the RabbitMQ build completes, **note the run number** of the successful build.
2. Update the `RUN_NUMBER` in the `project.env` file to match the build run number:

   ```env
   RUN_NUMBER=42
   ```
3. Run:

   ```bash
   gmake generate-manifest
   ```

   This will generate one manifest file **per RabbitMQ version defined** in `.github/matrix.json`, located in the `priv/` folder.
   For example:

   ```
   priv/seven_stream_metrics_manifest_v4.1.2.json
   priv/seven_stream_metrics_manifest_v4.0.9.json
   ```
4. **Manually verify the manifest**, especially the `changed_modules` list, which includes all `.beam` files that were modified.
   Example manifest:

   ```json
   {
     "base_rmq_ref": "v4.1.2",
     "source_build_id": "local",
     "changed_modules": [
       {
         "archive_path": "./rabbit-4.1.2+1.g4ca63cb/ebin/rabbit_quorum_queue.beam",
         "filename": "rabbit_quorum_queue.beam",
         "md5hash": "WUg4tPV17N7848kYq0qYrQ=="
       }
     ]
   }
   ```
5. Once verified, commit and push the updated manifest.

### Step 3: Trigger Plugin Build

Commit and push your changes (e.g. the manifest update).
   * Push your changes and open a Pull Request, **or**
   * Manually trigger the plugin build from GitHub Actions by selecting the correct branch.

No other inputs are required — the build will automatically pick up the configuration from your branch.

### Build Artifacts

After the plugin build completes, go to the **Artifacts** section in the GitHub Actions workflow run.

You will see **multiple `.ez` and `.zip` files**, corresponding to the different RabbitMQ versions defined in `matrix.json`.

#### Examples:

* **Plugin `.ez` files**:

  ```
  seven-stream-metrics-rabbitmq-v4.1.2-ez
  seven-stream-metrics-rabbitmq-v4.0.9-ez
  ```

* **Module override `.zip` files**:

  ```
  seven-stream-metrics-rabbitmq-v4.1.2-modules
  seven-stream-metrics-rabbitmq-v4.0.9-modules
  ```

You can download and use these artifacts for testing or deployment.

## Release Process

To create a new release of the plugin:

1. **Create and push a Git tag** (e.g. `v1.0.0`):

   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

2. This will automatically trigger the **`release` GitHub Actions workflow**, which builds the plugin and attaches the artifacts to a new GitHub Release.

3. After the release is published, you can go to the **"Releases"** tab in GitHub and optionally **edit the description**, or add notes, if needed.
