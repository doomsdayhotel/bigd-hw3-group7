#! /usr/bin/env python
import time
from sys import getsizeof 

from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from collections import Counter

class MRBasket(MRJob):
    """
    A class to count item co-occurrence in shopping baskets
    """

    def mapper_get_session_items(self, _, line):
        """

        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value) pairs
        """
        start_time = time.time()

        # Split the line into fields
        user_id, date, item = line.split(',')
        # Group items by user and session
        yield (user_id, date), item

        self.increment_counter('Mapper', 'get_session_items', 1)
        elapsed_time = time.time() - start_time
        self.increment_counter('Time', 'Mapper Get Session Items Time (ms)', int(elapsed_time * 1000))


    def reducer_group_items_by_session(self, user_session, items):
        start_time = time.time()

        # Collect all items for each user session
        sorted_items = sorted(set(items))  # Convert to set to remove duplicates, then back to list

        size_in_bytes = getsizeof(sorted_items)
        self.increment_counter('Space', 'Reducer Group Items by Session Space (bytes)', size_in_bytes)

        # Emit all combinations of items
        for item_pair in combinations(sorted_items, 2):
            yield item_pair, 1
        
        elapsed_time = time.time() - start_time
        self.increment_counter('Time', 'Reducer Group Items by Session Time (ms)', int(elapsed_time * 1000))
      
    
    def reducer_count_item_pairs(self, item_pair, counts):
        start_time = time.time()

        # Aggregate counts for each item pair
        yield item_pair, sum(counts)

        elapsed_time = time.time() - start_time
        self.increment_counter('Time', 'Reducer Count Item Pairs Time (ms)', int(elapsed_time * 1000))
 

    def mapper_count_pairs(self, item_pair, count):
        start_time = time.time()

        # Emit each item with its pair and count
        item1, item2 = item_pair
        yield item1, (item2, count)
        yield item2, (item1, count)

        elapsed_time = time.time() - start_time
        self.increment_counter('Time', 'Mapper Count Pairs Time (ms)', int(elapsed_time * 1000))
    
    def reducer_find_most_frequent_cooccur(self, item, cooccurs):
        start_time = time.time()

        # Find the most frequently co-occurring item for each item
        # Counter takes an iterable, but we want to count pairs (item, count), so use dict
        cooccur_dict = dict(cooccurs)

        size_in_bytes = getsizeof(cooccur_dict)
        self.increment_counter('Space', 'Reducer Find Most Frequent Cooccur Space (bytes)', size_in_bytes)

        # Find the item with the maximum count (most frequent co-occurrence)
        if cooccur_dict:
            cooccur_item, max_count = max(cooccur_dict.items(), key=lambda x: x[1])
            yield item, (cooccur_item, max_count)
        
        elapsed_time = time.time() - start_time
        self.increment_counter('Time', 'Reducer Find Most Frequent Cooccur Time (ms)', int(elapsed_time * 1000))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_session_items,
                   reducer=self.reducer_group_items_by_session),
            MRStep(reducer=self.reducer_count_item_pairs),
            MRStep(mapper=self.mapper_count_pairs,
                   reducer=self.reducer_find_most_frequent_cooccur)
        ]
    


# this '__name__' == '__main__' clause is required: without it, `mrjob` will
# fail. The reason for this is because `mrjob` imports this exact same file
# several times to run the map-reduce job, and if we didn't have this
# if-clause, we'd be recursively requesting new map-reduce jobs.
if __name__ == "__main__":
    # this is how we call a Map-Reduce job in `mrjob`:
    MRBasket.run()
