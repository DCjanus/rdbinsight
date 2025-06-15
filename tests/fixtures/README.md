# Test Fixtures

This folder holds **pre-built RDB files** used by integration tests.

• Redis ≤2.4 encoded small hashes as **ZipMap**; newer versions don't.
• Our CI runs Redis ≥2.6, so it can't generate ZipMap dumps on demand.
• We therefore commit ready-made dumps (from
  <https://github.com/sripathikrishnan/redis-rdb-tools/tree/master/tests/dumps>)
  to keep that code path covered.
