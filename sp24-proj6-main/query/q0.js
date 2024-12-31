// Task 0 (Building your first query)

db.ratings.aggregate([
    {
        $match: {
            timestamp: {
                $gte: 838857600,
                $lt: 849398400
            }
        }
    },
    {
        $group: {
            _id: "$movieId",
            min_rating: { $min: "$rating" },
            max_rating: { $max: "$rating" },
            count: { $sum: 1 }
        }
    },
    {
        $sort: { count: -1, _id: 1 }
    },
    {
        $limit: 10
    },
    {
        $lookup: {
            from: "movies_metadata",
            localField: "_id",
            foreignField: "movieId",
            as: "movies"
        }
    },
    {
        $project: {
            _id: 0,
            title: { $first: "$movies.title" },
            num_ratings: "$count",
            max_rating: 1,
            min_rating: 1
        }
    }
]);
