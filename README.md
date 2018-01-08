This repository contains a demonstration of an approach benchmarking CM-Well.

This works by 
- Generating data and inserting it into a CM-Well instance.
- A series of Gatling scenarios are run that use this generated data.
- Gatling writes detailed results for each scenario to file.
- Summary statistics are extracted from the detailed Gatling results which are used
as a basis for comparison between runs.

Since this approach involves loading data into CM-Well, this can be used as
an opportunity to measure ingestion rates (persist/index) rates.
The approach used here didn't return reliable results,
so this part needs to be revisited.


Limitations of this implementation:
- Should remove the generated data once the tests are complete.
- The command line parameters in BenchmarkRunner are not hooked up,
and will always:
  - Use localhost:9000 as the CM-Well instance.
  - Create 10,000 infotons.
  - Put the results in a directory with the prefix "results-". 
  
In addition to the detailed Gatling results, a benchmark-summary.json file will be placed
in the results directory that looks something like this:

```
  [{
    "simulation": "Get",
    "responseTime": 13,
    "requestsPerSecond": 4323.688524590164
  }, {
    "simulation": "GetWithData",
    "responseTime": 18,
    "requestsPerSecond": 5239.606557377049
  }, {
    "simulation": "Search",
    "responseTime": 48,
    "requestsPerSecond": 2007.0819672131147
  }, {
    "simulation": "SearchWithData",
    "responseTime": 42,
    "requestsPerSecond": 2240.7704918032787
  }]
```