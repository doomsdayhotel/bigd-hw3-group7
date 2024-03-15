# Part 5: Item co-occurrence

## Explain each step of your solution
* mapper_get_session_items: Emit the session identifier (user_id, date) and the item name.
* reducer_group_items_by_session: Aggregate items bought in the same session.
* mapper_item_pair: Emit all possible item pairs within each session.
* reducer_count_item_pair: Count item pairs.
* reducer_find_most_common_cooccur: Find the most common co-occurring item for each item.

## What problems, if any, did you encounter?
At first, I forgot to remove the duplicates in the `reducer_group_items_by_session` function.

## How does your solution scale?

Analyze the time and space used by each stage of your solution, including the number of (intermediate) output values.
* Time complexity: O(n^2), because I used 2 for loops in my `mapper_item_pair` function
* Space complexity: O(n). This is largely dependent on the number of unique item pairs generated. 
* Intermediate file: I have 46 intermediate files for the basket. 
