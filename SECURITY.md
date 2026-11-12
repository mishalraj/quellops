# Security policy

Please report vulnerabilities privately through GitHub Security Advisories. Do not
open a public issue containing credentials, exploit details, customer logs, cluster
data, or provider responses.

The latest release receives security fixes. QuellOps handles infrastructure data, so
deploy the control plane on a private network, use generated tokens, enable TLS for
traffic that crosses a trusted network boundary, and rotate any credential that may
have been exposed.

Include the affected version, configuration, reproduction steps, and impact in a
report. Maintainers will acknowledge a report within seven days and coordinate a fix
and disclosure timeline with the reporter.
