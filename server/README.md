# server

This folder contains the server main process and infrastructure.
It is used during development to figure out how to do things.

## Configuration

### Environment Config

To configure the dandelion server there are the following additional parameters

- `RUST_LOG` the rust environment logger we are using can be used to set the log level.
    For more information refer to the rust log crate.
    Per default it is set to `warn`, possible values are:
  
  - `error`
  - `warn`
  - `info`
  - `debug`
  - `trace`
  - `off`

- `NUM_TOTAL_CORES` the total number of cores available to the server.
    Expects an integer number.
    Defaults to using all cores.
- `NUM_DISP_CORES` the total number of cores to be used for the http frontend and the dispatcher. Defaults to 1.
- `DANDELION_TIMESTAMP_COUNT` the number of timestamps to preallocate.
    Expects a integer number.
    Defaults to 1000.
- `DANDELION_CONFIG` the path in which to look for the config file
    Defaults to `./dandelion.yml`

### Config File

To configure the dandelon using a config file.
The file is first searched in the full path given by `DANDELION_CONFIG`, then a file called `dandelion_config.json` is searched in the current working directory and last in the directory there the binary resides.

The config file has the expected layout as follows:

```json
{
    "total_cores": <NUM>,
    "dispatcher_cores": <NUM>,
    "timestamp_count": <NUM>,
    "log_level": <level>
}
```

### Precedence

The precedence is config file > environment variables > defaults

## Additional Settings

Some settings are also done through the enviroment variables, that have not yet been easily portet to the config struct.
Specifically:

- `PROCESS_WORKER_PATH` set the path for the process engine to find the worker binary
