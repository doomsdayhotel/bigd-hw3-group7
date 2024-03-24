# Part 5: Item co-occurrence

## Explain each step of your solution

1. Mapper: mapper_get_session_items
   The goal is to group items by user session/basket.
   The input would be raw text lines from the csv files. The text lines will be split into 'user_id', 'date', and 'item'.
   It emits tuples where '(user_id, date)' is the key and 'item' is the value. Each unique combination of the key '(user_id, date)' consititutes a user session/basket.

2. Reducer: reducer_group_items_by_session
   The goal is to aggregate items bought during the same user session/basket and emit unique item pairs fpr all the sessions.
   The input would be tuples of '(user_id, date)' and lists of 'item' from the mapper.
   The output would be unique item pairs with the count of 1, which would serve as the intermediate key-value pairs for the next step.

3. Reducer: reducer_count_item_pairs
   The goal is to count the occurrence of each item pair across all user sessions.
   The input would be unique item pairs and counts from the previous step.
   It sums the counts for each unique item pair across baskets and emits item pairs with their aggregated co-occurrence counts.

4. Mapper: mapper_count_pairs
   The goal is to output co-occurence key value pairs for final processing.  
   The input is item pairs with their occurrence counts from the previous reducer.
   For each item pair and count, emit two key-value pairs so that both items are keys, associating the count with the other item. Essentially, we take one item from the item pairs as the key first and then reverse. The will output key-value pairs where each item is a key, as well as the value in the form of the co-occuring item and the co-occurence count.

5. Reducer: reducer_find_most_frequent_cooccur
   The goal is to identify the most frequent co-occuring item for each item across user sessions.
   The input is key-value pairs from the previous mapper. The key is an item, and the value is a tuple that includes the co-occuring tem and the count.
   It stores the co-occuring items and counts in a dictionary and then for each key item, it finds the co-occuring item with the largest count. This way, it emits the most frequent co-occurring item and its count for each key item.

...

## What problems, if any, did you encounter?

1. At first, I forgot to remove the duplicates in the reducer_group_items_by_session function. (~ Iris) I might've missed this too if this wasn't brought up in our group meeting.(~Bess)

2. We three both did this question and got three versions of output. It was difficult figuring out how that happened(espeically when it comes to comparing each other's code because for me sometimes it is easier to understand my own harder to understand other people's logic; we also got stuck discussing/debating if different code could end up doing the same thing). What made it extra challenging was that the majority of our results align and only a small chunk was off.

3. For the next question about time and space used by each stage of our solution, I was stuggling to work out a coding solution to answer it more precisely.

...

## How does your solution scale?

Analyze the time and space used by each stage of your solution, including the number of (intermediate) output values.

1. Mapper: mapper_get_session_items
   Time: O(N); N is the number of lines in the input file, each line is processed once
   Space: O(U); U is the number of unique (user_id, date) combinations (namely, user sessions/baskets.)
   Intermediate output: emits one key-value pair per input line. The total number of output values is N.

2. Reducer: reducer_group_items_by_session
   Time:O(M log M); M is the number of items per session/basket.
   Space:O(M), where M is the maximum number of items in any session.
   Intermediate output: O(M choose 2), proportional to M^2 for large M since the output is all possible combinations of items as keys.

3. Reducer: reducer_count_item_pairs
   Time: O(K); K is the number of unique item pairs across all user sessions, where the counts are aggregated by their key.
   Apace: O(K) because the unique item pairs and the counts need to be stored.
   Intermediate output: K since every unique item pair generates one value.

4. Mapper: mapper_count_pairs
   Time: O(K) since each pair is processed once.
   Space: O(K); similar to the previous step, the item pairs are stored.
   Intermediate Output Values: 2K because we count the input item pairs two way, meaning that we emit two values for each input item pair.

5. Reducer: reducer_find_most_frequent_cooccur
   Time: O(J), where J is the number of items because the reducer runas once for each item to find the most frequent co-occuring item.
   Space: O(L), where L is the number of co-occurring items for an item. All pairs need to be stored to find the co-occuring item with the largest count.
   Intermediate Output Values: J; one per item.

...
