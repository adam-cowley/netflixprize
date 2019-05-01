from datetime import datetime
import fileinput

class ImportWriter:
    def __init__(self):
        # Statistics
        self.movie_count = 0
        self.review_count = 0
        self.user_count = 0

        # Batching
        self.movie_id = False

        self.movie_ids = []
        self.user_ids = []

        self.movie_prefix = "m"
        self.user_prefix = "u"


        self.movies = open("admin/movies.csv", "w")
        self.movies.write(":ID,id\n")

        self.users = open("admin/users.csv", "w")
        self.users.write(":ID,id\n")

        self.reviews = open("admin/reviews.csv", "w")
        self.reviews.write(":START_ID,:END_ID,rating:float,date:date\n")

    def read_line(self, line):
        if ":" in line:
            # movie_count Count
            self.movie_count = self.movie_count + 1

            # Get ID
            parts = line.split(":")
            self.movie_id = self.movie_prefix + parts[0]

            # Add to Movies File
            self.movies.write(",".join([self.movie_id, parts[0]]) + "\n")

            # Add to Movie IDs
            self.movie_ids.append(self.movie_id)
            
        else:
            self.review_count = self.review_count + 1

            parts = line.split(",")

            user_id = self.user_prefix + parts[0]

            # Add to User File
            if user_id not in self.user_ids:
                self.user_count = self.user_count + 1

                self.user_ids.append(user_id)

                self.users.write(",".join([user_id, parts[0]]) + "\n")

            # Add to Review File
            self.reviews.write( ",".join([ user_id, self.movie_id, parts[1], parts[2].strip() ]) + "\n" )

        if self.review_count % 10000 == 0:
            print(datetime.now())
            print("Movies ", self.movie_count)
            print("Ratings", self.review_count)
            print("Users  ", self.user_count, end="\n\n")


    def import_reviews(self, filename):
        for line in fileinput.input(filename):
            self.read_line(line)

        print(filename)
        print("Movies ", self.movie_count)
        print("Ratings", self.review_count)
        print("Users  ", self.user_count, end="\n\n")

    def close(self):
        self.movies.close()
        self.users.close()
        self.reviews.close()


res = ImportWriter()

res.import_reviews("import/combined_data_1.txt")
res.import_reviews("import/combined_data_2.txt")
res.import_reviews("import/combined_data_3.txt")
res.import_reviews("import/combined_data_4.txt")

res.close()