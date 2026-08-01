# Security policy

Punah is experimental and is not yet recommended for production or for storing
sensitive data. No release currently receives a formal security-support window.

Please do not open a public issue for a suspected vulnerability. Report it
privately through GitHub's security-advisory feature for this repository. Include
the affected revision, reproduction steps, impact, and any proposed mitigation.

The project treats durable-format validation, protocol bounds, unsafe memory
boundaries, fencing, and crash consistency as security-relevant surfaces. See
[`docs/safety.md`](docs/safety.md) for the engineering policy.
