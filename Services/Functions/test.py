import os
from monday import MondayClient 


token = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjI3NjgzMjkxMSwiYWFpIjoxMSwidWlkIjo0NzQ3NDA4NSwiaWFkIjoiMjAyMy0wOC0yM1QwMToyODowMC4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTM4MTUzMTgsInJnbiI6InVzZTEifQ.FWsMUEAmJUxJbKGpz5L3LW-29W2EFCsUJ_X4V_TWT-0'

client = MondayClient(
    token=token,
)
print(dir(client.boards))

print(client.boards.fetch_boards_by_id(8691846626))