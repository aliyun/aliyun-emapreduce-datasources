## v1.0.5

- [Improvement]: Modify `LoghubUtils` interface, and optimize the input parameters.
- [Improvement]: Output data of LogStream with JSON format, and add `__topic__` and `__source__` fileds.
- [Improvement]: Add configuration of loghub data fetching interval, i.e "spark.logservice.fetch.interval.millis", 200 default.
- [Improvement]: upgrade ODPS SDK to 0.20.7-public.

## v1.0.4

- [Bug Fix]: Fix the dependency comflict of guava, degrade guava to 11.0.2
- [Improvement]: Support >5GB output of task

## v1.0.3

- [Improvement]: Add configuration support for OSS Client.

## v1.0.2

- [Bug Fix]: Exception when parse OSS URI.

## v1.0.1

- [Improvement]: Optimize the definition of OSS URI.
- [New Feature]: Add the support of ONS.
- [New Feature]: Add the support of LogService.
- [Improvement]: Appendable write for OSS.
- [Improvement]: Add the support of `Multipart Upload` way to upload file to OSS.
- [Improvement]: Add the support of `Upload Part Copy` way to copy OSS object.