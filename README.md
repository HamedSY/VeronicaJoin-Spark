# Veronica Join: A Map-Reduced-Based Set Similarity Join Algorithm 

## Functionality
The code computes the similarity between two collection of sets (two different sets or self-join on a single dataset) and print the number of similar pair of sets. It uses Spark Framework for improving the efficiency of processing large datasets.

## How to use
- Place your dataset(s) in `data` directory (Each line represents a set and the tokens of the set are separated by whitespace).
- Set the your desired `threshold` for similarity checking in `Main` (The code uses Jaccard Similarity).
- Edit Spark configs for improving the efficieny based on your system/clusters spec.
- Everything is set up! Now just run `sbt run` to see the number of similarity pairs.
