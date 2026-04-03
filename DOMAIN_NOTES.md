# Domain Notes – Data Contract Enforcer

## 1. What types of data quality issues does the system detect?

The system detects:

- Schema violations (missing fields, type mismatches)
- Range violations (e.g. confidence outside 0–1)
- Statistical drift (mean deviation using z-score)
- Enum violations
- Format violations (UUID, datetime)
- AI-specific issues:
  - confidence drift (scale change)
  - output schema mismatch
  - input structure issues

---

## 2. What was the root cause of the failure?

The root cause was a **confidence scaling bug**:

- Expected: values in range **0.0–1.0**
- Observed: values in range **71–91**

This indicates a transformation from probability to percentage without updating the contract.

---

## 3. How did the system detect it?

The system detected it through multiple mechanisms:

1. **Range check**
   - FAILED (CRITICAL)
   - Values exceeded max=1.0

2. **Statistical drift**
   - FAILED (HIGH)
   - Mean shifted drastically from baseline

3. **AI validation**
   - Detected abnormal confidence distribution
   - Flagged scale shift (0–100 vs 0–1)

---

## 4. What is the blast radius of the failure?

The failure affects:

- Downstream node: `week4/cartographer`
- Estimated affected records: 5

This shows propagation of corrupted data into downstream systems.

---

## 5. How can this issue be prevented?

Recommended actions:

- Enforce contract validation in CI/CD before deployment
- Add pre-release checks for confidence scale consistency
- Monitor statistical drift continuously
- Implement schema versioning and compatibility checks
- Block downstream consumption on critical failures

---

## Key Insight

This project demonstrates that:

> Data contracts are not just schema checks — they are **runtime guarantees for system behavior**, especially for AI systems.

The Enforcer successfully caught a silent failure that would otherwise corrupt downstream analytics and decisions.
## Final System Improvements

After the interim phase, the system was extended to include:

- Full contract coverage across all pipeline stages (Week3 → Week5)
- Breaking schema evolution detection with migration planning
- Enhanced AI validation including entity consistency checks
- Improved attribution with real lineage mapping
- CI/CD integration for automated validation enforcement

These improvements transformed the system from a prototype into a more production-ready data contract enforcement framework.