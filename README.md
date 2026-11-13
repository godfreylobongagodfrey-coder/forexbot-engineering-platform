# ForexAI Engineering Platform

A Python engineering platform demonstrating read-only market-data observation, data integrity, operational monitoring, audit logging and task supervision.

## Overview

This project explores the engineering foundations required to build reliable data-collection and monitoring systems around external market-data sources.

The public repository focuses on reusable infrastructure rather than trading strategies or automated trade execution.

## Key Capabilities

- Market-data observation and validation.
- Persistent observation storage.
- Duplicate and chronological data checks.
- Immutable database records.
- Operational health monitoring.
- Structured audit logging.
- Task scheduling and recovery.
- Automated unit testing.

## Technology

- Python
- SQLite
- Python standard library
- unittest

## Running Tests

From the project root, run:

    python -m unittest discover -s tests -v

## Project Structure

    forexai/
        audit.py
        models.py
        mt5_observation.py
        operations.py
        sqlite_support.py
        supervisor.py

    tests/
        test_audit.py
        test_observation_store.py
        test_operations.py
        test_supervisor.py

## Scope and Confidentiality

This repository is a deliberately limited public engineering portfolio.

It excludes proprietary trading strategies, strategy research, private trading parameters, account information, credentials and private execution components.

## Safety and Project Status

The project focuses on read-only observation and engineering validation.

It does not claim live-trading readiness, profitability or guaranteed market-data availability.

The included tests validate specific software behaviours; they do not establish production readiness.

## Development Principles

- Explicit system boundaries
- Data integrity and traceability
- Defensive validation
- Operational observability
- Automated testing
- Separation of public infrastructure from proprietary logic
