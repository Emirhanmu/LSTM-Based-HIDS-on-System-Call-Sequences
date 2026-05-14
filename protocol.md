# Final Evaluation Protocol

## Study scope
- Dataset: LID-DS 2021
- Scenario: PHP_CWE-434
- Study type: single-scenario case study
- Detection setting: offline trace-level anomaly detection

## Data handling
- Train / validation / test splits are trace-disjoint.
- No trace-name overlap exists across splits.
- Validation attack traces and test attack traces are disjoint.
- Training uses only normal windows.

## Sequence construction
- Grouping unit: thread_id
- Window size: 30
- Stride: 1
- Boundary-crossing windows are discarded.

## Models
- STIDE (baseline, n-gram size = 6)
- LSTM syscall-only
- LSTM context-aware

## Training setup
- Training subset size for LSTM models: 500,000 windows
- Purpose: balance representation coverage and computational cost

## Decision level
- Window scores are aggregated at trace level.
- Trace score = mean of top 10% highest window scores.

## Thresholding policy
### Primary operating point
- Validation-normal-based q95 threshold

### Sensitivity analysis
- Validation-normal-based q90 threshold

### Reported for completeness
- Original anomaly-based validation thresholding results

## Fixed q95 thresholds
- STIDE q95 threshold = 0.145818
- LSTM syscall q95 threshold = 0.895200
- LSTM context q95 threshold = 1.067240

## Fixed q90 thresholds
- STIDE q90 threshold = 0.054867
- LSTM syscall q90 threshold = 0.575347
- LSTM context q90 threshold = 0.915675

## Final reporting strategy
- Main comparison: q95 results
- Sensitivity comparison: q90 results
- Original thresholding results: appendix / supporting table

## Important limitations
- Small number of anomalous validation traces
- Single-scenario study
- Offline trace-level setting
- Training subset capped at 500,000 windows