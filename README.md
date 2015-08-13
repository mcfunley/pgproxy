## A story explaining what this is and why you probably don't want to use it.

At Etsy, it made a lot of sense for us to have a giant test suite with selenium early on, because we inherited a big messy codebase that wasn't testable any other way. And we also had no monitoring on production yet. So this was the only way we had to gain confidence in something we were going to push out. It was during this era that I wrote pgproxy.

Eventually, we instrumented the app and built out some really great production monitoring. At that point we changed strategies: we deployed smaller sets of changes more frequently, and if our nagios checks went awry we took changes back out. And we also started doing [feature flags](https://github.com/etsy/feature), so that we could deploy changes to production without having them change the executing code. And we could also turn on features a few percentage points at a time.

That might sound like a riskier approach but it's not. In practice it's not possible to prevent all problems, and you have to have the production monitoring. Quick detection and mitigation is much better prevention, at least in our case. 

So we just deleted the big selenium test suite, because it was no longer providing us with the confidence in deployments that we wanted. Much to the contrary--it was hard to make tests like that that weren't flaky, and when tests flake all that happens is you lose confidence in the accuracy of the suite and you stop paying attention to it. Tests that flake are worse than no tests at all, I've found.

Over time we rebuilt a test suite, but it was 90% unit tests. That is, tests that mock things and don't use an external database or other services. We also built out a couple of selenium tests, but only in the super critical paths (for Etsy that meant checkout and user registration). In those places, the maintenance costs for a few tests was worth it, because breakage was expensive.

## More on the original rationale for doing this, and documentation if you must use it
See here: [PGProxy: A Testing Proxy for Postgres](http://mcfunley.com/469/pgproxy-a-testing-proxy-for-postgres)

