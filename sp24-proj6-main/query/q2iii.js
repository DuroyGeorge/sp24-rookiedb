// Task 2iii

db.movies_metadata.aggregate([
  {
    $project: {
      budget: {
        $cond: {
          if: {
            $and: [
              {$ne: ['$budget', null]}, {$ne: ['$budget', '']},
              {$ne: ['$budget', false]}, {$ne: ['$budget', undefined]}
            ]
          },
          then: {
            $cond: {
              if: {$isNumber: '$budget'},
              then: '$budget',
              else: {
                $toInt: {
                  $trim: {
                    input: {$trim: {input: '$budget', chars: '\\$'}},
                    chars: ' USD'
                  }
                }
              }
            }
          },
          else: 'unknown'
        }
      }
    }
  },
  {
    $project: {
      budget: {
        $cond: {
          if: {$eq: ['$budget', 'unknown']},
          then: 'unknown',
          else: {
            $multiply: [{$round: {$divide: ['$budget', 10000000]}}, 10000000]
          }
        }
      }
    }
  },
  {$group: {_id: '$budget', count: {$sum: 1}}},
  {$project: {_id: 0, budget: '$_id', count: 1}}, {$sort: {budget: 1}}
]);