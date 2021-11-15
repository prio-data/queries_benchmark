
# Queries benchmark

This script is meant as a benchmark to make sure that ViEWS 3 is working as expected.
Run as a script:

```
python trials.py
```

## Testing local ViEWS3

To test a local version of ViEWS3, you need to set the `REMOTE_URL` viewser
configuration setting to the local URL of your instance. Using the views3
repository and compose file, this url is `http://0.0.0.0:4000`, so you could
set this correctly by running:

```
viewser config set REMOTE_URL http://0.0.0.0:4000
```
