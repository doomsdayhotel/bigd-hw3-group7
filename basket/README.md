# Part 5: Item co-occurrence

## Explain each step of your solution

1. Parse data to generate tuples of each `basket` and `items`, where a single `basket` is identified as a unique `user` 
and `date` combination, since all activity by a user on the same day
corresponds to a single `basket`. 
2. Aggregate to get a set of all unique `items` for each (`user_id`, `date`) tuple (`basket`) to remove duplicates.
3. For each `basket`, generate all combination of `item` pairs and count co-occurrence.
4. Across all (`user_id`, `date`) combination `baskets` sum the co-occurrence count for each `item` pair.
5. Reformat so that key is `item_id` and value is (`item_id`, `co-occurrence count`) and the item with the highest 
co-occurrence count.

## What problems, if any, did you encounter?

...

## How does your solution scale?

Analyze the time and space used by each stage of your solution, including the number of (intermediate) output values.

...
