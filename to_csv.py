import datetime
import fileinput
# from neo4j import GraphDatabase

# driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))

# Movies: 4499
# Reviews: 24053765

# Stats
movies = 0
reviews = 0

# Import
movie_id = False


class CSVWriter:
    def __init__(self, batch_size=10000):
        # Config
        # self.driver = driver
        self.batch_size = batch_size

        # self.session = driver.session()

        self.create_new_file()

        # Statistics
        self.movies = 0
        self.reviews = 0
        self.batches = 0

        # Batching
        self.movie_id = False
        self.batch = []

    def create_new_file(self):
        self.writer = open( "cleaned/"+ str(datetime.datetime.now()) + ".csv", "a+")

    def read_line(self, line):
        if ":" in line:
            self.movies = self.movies + 1

            parts = line.split(":")
            self.movie_id = parts[0]
        else:
            self.reviews = self.reviews + 1

            parts = line.split(",")

            self.write(self.movie_id, parts[0], parts[1], parts[2])

    def write(self, movie, customer, rating, date):
        self.writer.write(movie +","+ customer +","+ rating +","+ date)


    def check_batch(self):
        if len(self.batch) >= self.batch_size:
            self.execute_batch()

    def execute_batch(self):
        self.writer.close()
        self.create_new_file()
    

    def import_reviews(self, filename):
        for line in fileinput.input(filename):
            self.read_line(line)

        print("Movies ", self.movies)
        print("Ratings", self.reviews)
        print("Batches", self.batches)

        self.writer.close()

  



res = CSVWriter(batch_size=10000)
res.import_reviews("import/combined_data_4.txt")