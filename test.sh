curl -X POST http://localhost:8101/a2a \
     -H "Content-Type: application/json" \
     -d '{
           "jsonrpc": "2.0",
           "method": "search_candidates",
           "params": {
             "title": "Data Scientist",
             "skills": "Python,Machine Learning"
           },
           "id": 99
         }'
