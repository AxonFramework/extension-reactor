# Axon Framework - Reactor Extension 

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.axonframework.extensions.reactor/axon-reactor/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.axonframework.extensions.reactor/axon-reactor/)
[![Build Status](https://travis-ci.org/AxonFramework/extension-reactor.svg?branch=master)](https://travis-ci.org/AxonFramework/extension-reactor)
[![SonarCloud Gate Status](https://sonarcloud.io/api/project_badges/measure?project=AxonFramework_extension-reactor&metric=alert_status)](https://sonarcloud.io/dashboard?id=AxonFramework_extension-reactor)

Axon Framework is a framework for building evolutionary, event-driven microservice systems,
 based on the principles of Domain Driven Design, Command-Query Responsibility Segregation (CQRS) and Event Sourcing.

As such it provides you the necessary building blocks to follow these principles. 
Building blocks like Aggregate factories and Repositories, Command, Event and Query Buses and an Event Store.
The framework provides sensible defaults for all of these components out of the box.

This set up helps you create a well structured application without having to bother with the infrastructure.
The main focus can thus become your business functionality.

Axon Framework Reactor Extension provides integration with [Project Reactor](https://projectreactor.io/). Currently, it
provides Gateways that utilize Project Reactor's types such as Mono and Flux. Having said that, ReactorCommandGateway,
ReactorEventGateway and ReactorQueryGateway are at your disposal for usage.
  
For more information on anything Axon, please visit our website, [http://axoniq.io](http://axoniq.io).

## Getting started

The [reference guide](https://docs.axoniq.io) contains a separate chapter for all the extensions.
The Reactor extension description can be found [here](https://docs.axoniq.io/reference-guide/extensions/reactor).

## Receiving help

Are you having trouble using the extension? 
We'd like to help you out the best we can!
There are a couple of things to consider when you're traversing anything Axon:

* Checking the [reference guide](https://docs.axoniq.io/reference-guide/extensions/reactor) should be your first stop,
 as the majority of possible scenarios you might encounter when using Axon should be covered there.
* If the Reference Guide does not cover a specific topic you would've expected,
 we'd appreciate if you could file an [issue](https://github.com/AxonIQ/reference-guide/issues) about it for us. 
* There is a [forum](https://discuss.axoniq.io/) to support you in the case the reference guide did not sufficiently answer your question.
Axon Framework and Server developers will help out on a best effort basis.
Know that any support from contributors on posted question is very much appreciated on the forum.
* Next to the forum we also monitor Stack Overflow for any questions which are tagged with `axon`.

## Feature requests and issue reporting

We use GitHub's [issue tracking system](https://github.com/AxonFramework/extension-reactor/issues) for new feature 
request, extension enhancements and bugs. 
Prior to filing an issue, please verify that it's not already reported by someone else.

When filing bugs:
* A description of your setup and what's happening helps us figuring out what the issue might be
* Do not forget to provide version you're using
* If possible, share a stack trace, using the Markdown semantic ```

When filing features:
* A description of the envisioned addition or enhancement should be provided
* (Pseudo-)Code snippets showing what it might look like help us understand your suggestion better 
* If you have any thoughts on where to plug this into the framework, that would be very helpful too
* Lastly, we value contributions to the framework highly. So please provide a Pull Request as well!
