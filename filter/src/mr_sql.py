#! /usr/bin/env python

from mrjob.job import MRJob
import re

MOVIE_RE = re.compile(r"(.*) \((\d{4})\), (.*)")


class MRMoviesByGenreCount(MRJob):
    """
    Find the distinct number of movies in specific genres over time
    """

    def mapper(self, _, line):
        """
        Parse each line in input file (e.g., movies.csv) and yield key (year, movie_genre) and value movie_name if
        movie_genre is 'Western' or 'Sci-Fi'.

        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value)
                key: (str, str) where key is a tuple of the form (year, movie genre)
                value: str where value is the movie name

        """
        # Find match object for each line
        if (match := re.match(MOVIE_RE, line)) is not None:
            movie_name, year, movie_genre = match.groups()
            # Yield key, value pair if move genre is 'Western' or 'Sci-Fi'
            if (movie_genre == 'Western') or (movie_genre == 'Sci-Fi'):
                yield (year, movie_genre), movie_name

    # optional: implement the combiner:
    # def combiner(self, key, values):
        # start using the key-value pairs to calculate the query result
        # pass

    def reducer(self, key, values):
        """
        Count the number of distinct movies for each year and movie genre.
        
        Parameters:
            key: (str, str)
                key is a tuple of the form (year, movie genre)
            values: list
                list containing movie name

            Yields:
                key: (str, str)
                    key is a tuple of the form (year, movie)
                value: int
                    Count of distinct movies for each year and movie
        """
        # use the key-value pairs to calculate the query result
        unique_movies = set(values)
        yield key, len(unique_movies)


if __name__ == '__main__':
    MRMoviesByGenreCount.run()
