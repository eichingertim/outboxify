# Security Policy

The Outboxify team takes the security of our software and users seriously. This document outlines our security policies, supported versions, and how to report vulnerabilities.

---

## Supported Versions

We provide security updates and patches for the following major versions:

| Version | Supported          | Security Patches |
| :------ | :----------------- | :--------------- |
| 2.x.x   | :white_check_mark: | Active           |
| 1.x.x   | :x:                | End of Life      |

---

## Reporting a Vulnerability

If you discover a security vulnerability within Outboxify, please **do NOT report it publicly** on GitHub Issues or discussions.

Instead, please report security issues privately via one of the following methods:

1. **GitHub Private Vulnerability Reporting**: Use the "Report a vulnerability" button under the **Security** tab of our GitHub repository.
2. **Email**: Send encrypted or plain text details to **[security@outboxify.io](mailto:security@outboxify.io)**.

### What to Include in Your Report

To help us triage and resolve the issue quickly, please provide:
- A description of the vulnerability and its potential impact.
- Affected runtime ecosystem(s) (Java, Node.js, Python) and database dialects.
- Step-by-step reproduction steps or a minimal proof of concept (PoC).
- Any proposed remediation or patch, if available.

---

## Response Timeline & Disclosure Process

- **Initial Response**: We will acknowledge receipt of your vulnerability report within **48 hours**.
- **Assessment & Triage**: We will confirm the vulnerability and provide an estimated timeline for remediation within **5 business days**.
- **Coordinated Release**: Once a fix is verified, we will release a patch release (`v2.x.x`) across Maven Central, npm, and PyPI, and publish a Security Advisory with attribution to the reporter.

---

## Security Best Practices for Deployments

- Always use dedicated database credentials with least-privilege permissions restricted to the outbox and domain tables.
- Enable TLS / mTLS and SASL authentication on Apache Kafka brokers in production environments.
- Monitor failed outbox records (`status = 'FAILED'`) through alerting pipelines.
