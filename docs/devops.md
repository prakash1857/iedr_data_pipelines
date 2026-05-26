# DevOps Approach — IEDR Data

## 1. Repository Strategy

**Single repo: `iedr_project`**

All the code are in one repossitory.

```
iedr_project/
|-- pipeleines/     # bronze / silver / gold
|-- schemas/        # ddl
```

---

## 2. Branch Model

```
main    <- production only; requires approval and change ticket, then deploy to Prod Databricks workspace
 |
dev     <- integration branch; merges auto deploy to Dev Databricks workspace
 |
feature   <- all day-to-day work with the jira story
 |
hotfix   <- emergency prod fix; merge to main + dev
```

## 3. SDLC Flow

```
1. Data Engineer cuts feature branch from dev
        │
2. Writes code + unit tests 
        │
3. Opens Pull Request to dev
        │
4. CI runs (GitHub Actions):
   └── pytest tests/unit
        │
5. Peer review (min 1 approval) → merge to dev
        │
6. auto-deploys asset bundle to Dev Databricks workspace
        │
7. Data QA in Dev:
   - Run end-to-end pipeline on sample S3 data
   - Verify silver circuit count << bronze segment count (U1 aggregation)
   - Spot-check gold tables with API queries
        │
8. Pull Request: dev → main → CI + integration test against Test workspace
        │
9. UAT sign-off (product owner validates gold)
        │
10. Merge → deploys asset bundle to Prod Databricks workspace
    Release → notify stakeholders
```

