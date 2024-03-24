# Part 5: Item co-occurrence

## Explain each step of your solution
Mapper: `mapper_get_session_items` The goal is to group items by user session/basket. The input would be raw text lines from the csv files. The text lines will be split into 'user_id', 'date', and 'item'. It emits tuples where '(user_id, date)' is the key and 'item' is the value. Each unique combination of the key '(user_id, date)' consititutes a user session/basket.

Reducer: `reducer_group_items_by_session` The goal is to aggregate items bought during the same user session/basket and emit unique item pairs for all the sessions. The input would be tuples of '(user_id, date)' and lists of 'items' from the mapper. The output would be unique item pairs.

Mapper: `mapper_item_pair` The goal is to generate pairs of items that occur together in the same basket. The input is item pairs with their occurrence counts from the previous reducer. For each item pair and count, emit two key-value pairs so that both items are keys, associating the count with the other item. Essentially, we take one item from the item pairs as the key first and then reverse. The will output key-value pairs where each item is a key, as well as the value in the form of the co-occuring item and the co-occurence count.

Reducer: `reducer_count_item_pair` The goal is to sum the number of times each item pair co-occurs across all sessions. The input is item pairs and their occurrence counts from the previous mapper. The output is the co-occurence item and the sum of it.

Reducer: `reducer_find_most_common_cooccur` The goal is to identify the most frequent co-occuring item for each item across user sessions. The input is key-value pairs from the previous reducer. The key is an item, and the value is a tuple that includes the co-occuring item and the count. It stores the co-occuring items and counts in a dictionary and then for each key item, it finds the co-occuring item with the largest count. This way, it emits the most frequent co-occurring item and its count for each key item.

## What problems, if any, did you encounter?
1. At first, I forgot to remove the duplicates in the `reducer_group_items_by_session` function.(~ Iris) I might've missed this too if this wasn't brought up in our group meeting.(~Bess) 
Also wan't aware of there might be multiple co-items with the highest co-currence count if it wasn't for the discussion in Discord. (~Iris)
2. We three both did this question and got three versions of output. It was difficult figuring out how that happened(espeically when it comes to comparing each other's code because for me sometimes it is easier to understand my own harder to understand other people's logic; we also got stuck discussing/debating if different code could end up doing the same thing). What made it extra challenging was that the majority of our results align and only a small chunk was off. (~Bess)

3. For the next question about time and space used by each stage of our solution, I was stuggling to work out a coding solution to answer it more precisely. (~Bess)

## How does your solution scale?

Analyze the time and space used by each stage of your solution, including the number of (intermediate) output values.
1. Stage 1
   1. Mapper: `mapper_get_session_items`: 
      * Time: O(nm) where n is the number of lines in the input file and m is the length of a line.
      * Space: O(m) for calling the function, where  m is the length of a line.
   2. Reducer: `reducer_group_items_by_session` 
      * Time: O(n) where n is the number of lines in the list. Converting to set take O(n) and converting back to list also takes at most O(n), which can be simplified to O(n).
      * Space: O(n) for calling the function, where n is the number of items in the input list.
   3. Number of intermediate output: 47

2. Stage 2
   1. Mapper: `mapper_item_pair`: 
      * Time: O(n^2). Generating all 2-item combinations from `item_list` is O(n^2) by C(n,2) = n(n-1)/2
      * Space: O(n) for calling the function as the input list of items itself takes O(n) space. 
   2. Reducer: `reducer_count_item_pair` : 
      * Time: O(n) where n is the length of counts. `sum()` function iterates through each element in the counts list once to compute the total sum.
      * Space: O(1) for calling the function as the amount of additional space required does not scale with the size of the input list `counts`.
   3. Number of intermediate output: 47
   

3. Stage3
   1. Reducer: `reducer_find_most_common_cooccur` 
      * Time: O(n). Conversion to dictionary takes O(n), finding the maximum count takes O(n), filtering items with maximum count takes O(n), and yielding results through for loop takes O(n). Overall time complexity is O(4n) which simplifies to O(n).
      * Space: O(n). Creating dictionary takes O(n) and list of most frequent co-occurrences could take up to O(n). Overall space complexity is O(2n) which simplifies to O(n).
   2. Number of final output: 47
