#! /usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep


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
        # Emit the session identifier (user_id, date) and the item name.
        user_id, date, item_name = line.split(',')
        session_id = (user_id, date)
        yield session_id, item_name
        '''
        example output
            ((1, '2022-10-31'), 'chocolates')
            ((1, '2022-10-31'), 'pumpkins')
            ((1, '2022-10-31'), 'eggs')
            ((2, '2022-10-31'), 'chocolates')
            ((2, '2022-10-31'), 'candy corn')
        '''

    def reducer_group_items_by_session(self, session_id, item_name):
        # Aggregate items bought in the same session.
        item_list = list(item_name)
        yield session_id, item_list
        '''
        example output
            ((1, '2022-10-31'), ['chocolates', 'pumpkins', 'eggs']
            ((2, '2022-10-31'), ['chocolates', 'candy corn'])
        '''

    def mapper_item_pair(self, session_id, item_list):
        # Emit all possible item pairs within each session.
        for i in range(len(item_list)):
            for j in range(i + 1, len(item_list)):
                item1 = item_list[i]
                item2 = item_list[j]
                yield (item1, item2), 1
                yield (item2, item1), 1
        '''
        example output
            (('chocolates', 'pumpkins'), 1)
            (('chocolates', 'eggs'), 1)
            (('pumpkins', 'eggs'), 1)
            (('pumpkins', 'chocolates'), 1)
            (('eggs', 'chocolates'), 1)
            (('eggs', 'pumpkins'), 1)
            (('chocolates', 'candy corn'), 1)
            (('candy corn', 'chocolates'), 1)
        '''

    def reducer_count_item_pair(self, item_pair, counts):
        # Count item pairs.
        yield item_pair[0], (item_pair[1], sum(counts))

        '''
        example output
            ('chocolates', ('pumpkins', 1))
            ('chocolates', ('eggs', 1))
            ('chocolates', ('candy corn', 1))
            ...
        '''
        
    def reducer_find_most_common_cooccur(self, item, cooccur_items):
        # Find the most common co-occurring item for each item.
        yield item, max(cooccur_items, key=lambda x: x[1])
        '''
        example output
            ('chocolates', ('candy corn', 1))
            ...
        '''


    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_session_items,
                reducer=self.reducer_group_items_by_session
            ),
            MRStep(
                mapper=self.mapper_item_pair,
                reducer=self.reducer_count_item_pair
            ),
            MRStep(
                reducer=self.reducer_find_most_common_cooccur
            )
        ]


# this '__name__' == '__main__' clause is required: without it, `mrjob` will
# fail. The reason for this is because `mrjob` imports this exact same file
# several times to run the map-reduce job, and if we didn't have this
# if-clause, we'd be recursively requesting new map-reduce jobs.
if __name__ == "__main__":
    # this is how we call a Map-Reduce job in `mrjob`:
    MRBasket.run()
