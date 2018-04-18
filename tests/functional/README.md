# Functional tests

This is a temp README! This is a brain dump of notes + how to while this branch is a WIP.

## Setup

### pubsub emulator

1. In one terminal window, run: `gcloud beta emulators pubsub start --host-port=localhost:8085 --project=a-project` - make sure the project matches what's in the `config` dict in both the `*.py` files.
2. In another terminal window - the one that you'll be running the scripts from - run (outside of the virtualenv) `$(gcloud beta emulators pubsub env-init)`.

### package setup

1. Grab the patch: `git fetch origin` followed by `git checkout -b feature/add-event-consumer origin/feature/add-event-consumer`.
2. Within a test virtualenv for gordon-gcp, `pip install -r dev-requirements`
3. That should install all the deps, but just in case, running these scripts require `google-api-core==1.1.0` and `google-cloud-pubsub==0.33.0`.

### testing

**NOTE**: these need to be in the terminal window that `$(gcloud beta emulators pubsub env-init)` was ran! Else, it'll connect externally.

1. Run `pytest -s -v tests/functional` to watch messages get consumed and then "routed" & acked. **NOTE**: there's a bug that the first run doesn't work, but subsequent ones do.


## Notes

### testing setup
- assert emulator is running, else bail out
- load config
- create publisher client
- load sample messages
    + valid & invalid of both audit log & event
- publish sample messages
    + might need to sleep(1)?
- start event consumer

### test pathes
- happy paths audit log message and event message
    + phase is updated to 'consumed'
    + added to success channel
    + fake router updates phase to 'done' to call 'cleanup'
     message is acked/no longer in pubsub
        * assert message history log
- invalid json encoded data
- invalid data against schema
- cleanup: all msgs are acked

### ideas/todos
- fix bug of when tests fail, it shuts down properly and not hang
- moar logging! because wtf is going on!
- think of a better reorg of `gordon-gcp/tests/functional/` for reusability
- add functional test section to `tox.ini` - this is there already, just untested/might not work
- any consideration for threadpool executors? flow control?
- specific toml for test config?
- message publisher fixture/func

### setup for travis

- docker compose
    + http://elliot.land/post/using-docker-compose-on-travis-ci
    + https://mike42.me/blog/how-to-set-up-docker-containers-in-travis-ci
    + https://github.com/mike42/minimal-docker-ci
