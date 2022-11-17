[![Project stage][Stage]][Stage-Page]
[![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/dev.zio/zio-kafka_2.13?server=https%3A%2F%2Foss.sonatype.org)](https://oss.sonatype.org/content/repositories/snapshots/dev/zio/zio-kafka_2.13/)

# Conduktor-only section

## How to work on this fork️

🙂️ Before to start to work on this repo, be sure to read this paragraph, please 🙂️

We, at Conduktor, maintain a fork of the `zio/zio-kafka` repo so that we can release new versions as soon as we need them.

For that purpose and to adapt this repo to our needs, we created a new main branch, named `zio2-main`, which is configured to be our default branch.    
This `zio2-main` is based on the `master` branch and MUST be rebased on it when the `master` branch is updated.

Our `master` branch represents and tracks the `zio/zio-kafka` `master` branch.   
We MUST NOT push/merge anything specific to Conduktor in our `master` branch.   
In order to keep this fork up-to-date with `zio/zio-kafka`, when the ZIO devs merge things in their `master` branch, we MUST rebase our `master` branch on their `master` branch and then, rebase our `zio2-main` branch on our `master` branch.

If you want to add some Conduktor-only things (CI stuff for example) to this fork, then you MUST start your new branch from our `zio2-main` branch.   
If you want to add a feature to the official `zio/zio-kafka` repo, then you MUST start your new branch from our `master` branch.

## Versioning scheme

For the ZIO2 versions of our fork of zio-kafka, here’s the versioning pattern to follow:

`<condensed base zio-kafka version>-<our version of the lib>-cdk`

in which:
- the `<condensed base zio-kafka version>` is, for example and for now, `201` because our fork is based on the version `2.0.1` of zio-kafka
- the `<our version of the lib>` is for now `1.0.0`, as it’s our first version.

I hope this versioning will be flexible enough, so we can express anything we want and/or need and will allow us to easily track/stay up-to-date with the version of the upstream repo.

## How to release a new version of this fork

The CI/CD pipeline of this fork has been adapted to follow the usual Conduktor CI/CD process.

To release a new version of this library, you just need to create a new release in this repo.   
This will trigger a Github Actions pipeline that will publish the new version.

Please follow the versioning scheme described above.

# Welcome to ZIO Kafka

ZIO Kafka provides a purely functional, streams-based interface to the Kafka
client. It integrates effortlessly with ZIO and ZIO Streams.

## Contents

- [Getting Started](docs/gettingStarted.md)
- [Getting help](#getting-help)
- [Credits](#credits)
- [Legal](#legal)

## Getting help

Join us on the [ZIO Discord server](https://discord.gg/2ccFBr4) at the `#zio-kafka` channel.

## Credits

This library is heavily inspired and made possible by the research and implementation done in [Alpakka Kafka](https://github.com/akka/alpakka-kafka), a library maintained by the Akka team and originally written as Reactive Kafka by SoftwareMill.

## Legal

Copyright 2021 Itamar Ravid and the zio-kafka contributors. All rights reserved.
<!-- TODO: not all rights reserved, rather Apache 2... -->

[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-kafka_2.12/ "Sonatype Releases"
[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-kafka_2.12.svg "Sonatype Releases"
[Stage]: https://img.shields.io/badge/Project%20Stage-Production%20Ready-brightgreen.svg
[Stage-Page]: https://github.com/zio/zio/wiki/Project-Stages
