## What are DataFrames?

To begin your journey with `cuallee` is important to denote what is the data structure used for data validation.


???+ info "DataFrame"
    Is a two-dimensional, size-mutable, and potentially heterogeneous tabular data structure with labeled axes (rows and columns) in Python, provided by the pandas library. It is one of the most commonly used data structures for data manipulation and analysis in Python. 

### Key Features of a DataFrame:

1. **Two-Dimensional**: It represents data in a table-like format with rows and columns, similar to a spreadsheet or SQL table.

2. **Labeled Axes**: Each row and column in a DataFrame has a label. Columns are labeled with column names, and rows are labeled with an index.

3. **Heterogeneous Data**: Different columns can contain different types of data (e.g., integers, floats, strings, etc.).

4. **Size-Mutable**: The size of the DataFrame can be changed; you can add or drop rows and columns as needed.

5. **Powerful Data Alignment**: DataFrames allow for complex operations on data sets, including handling missing data, merging/joining different data sets, and reshaping data.

### Creating a DataFrame

You can create a DataFrame in several ways, such as from a dictionary of lists, lists of dictionaries, or reading data from a file (e.g., CSV, Excel).

#### Example 1: Creating a DataFrame from a Dictionary of Lists

```python
import pandas as pd

data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['New York', 'San Francisco', 'Los Angeles']
}

df = pd.DataFrame(data)
print(df)
```

#### Example 2: Creating a DataFrame from a List of Dictionaries

```python
import pandas as pd

data = [
    {'Name': 'Alice', 'Age': 25, 'City': 'New York'},
    {'Name': 'Bob', 'Age': 30, 'City': 'San Francisco'},
    {'Name': 'Charlie', 'Age': 35, 'City': 'Los Angeles'}
]

df = pd.DataFrame(data)
print(df)
```

#### Example 3: Reading a DataFrame from a CSV File

```python
import pandas as pd

df = pd.read_csv('data.csv')
print(df)
```

### Basic Operations on DataFrames

#### Accessing Data

- **Selecting a Column**: `df['ColumnName']` or `df.ColumnName`
- **Selecting Multiple Columns**: `df[['Column1', 'Column2']]`
- **Selecting Rows by Index**: `df.loc[index]` or `df.iloc[index]` for integer-location based indexing

#### Adding and Dropping Columns

- **Adding a Column**: `df['NewColumn'] = value`
- **Dropping a Column**: `df.drop('ColumnName', axis=1, inplace=True)`

#### Filtering Data

- **Filtering Rows Based on Condition**: `df[df['ColumnName'] > value]`

### Example: Basic DataFrame Operations

```python
import pandas as pd

# Creating the DataFrame
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['New York', 'San Francisco', 'Los Angeles']
}

df = pd.DataFrame(data)

# Selecting a column
print(df['Name'])

# Adding a new column
df['Salary'] = [70000, 80000, 90000]

# Dropping a column
df.drop('City', axis=1, inplace=True)

# Filtering rows where Age is greater than 28
filtered_df = df[df['Age'] > 28]

print(filtered_df)
```

### Advanced Features

- **Group By**: Allows you to group data and perform aggregate functions.
- **Merge/Join**: Combining multiple DataFrames based on a common key.
- **Pivot Tables**: Creating pivot tables for summarizing data.
- **Handling Missing Data**: Functions to detect, remove, or fill missing values.

DataFrames are a powerful and flexible data structure for data analysis, and mastering their use can greatly enhance your ability to work with data in Python.