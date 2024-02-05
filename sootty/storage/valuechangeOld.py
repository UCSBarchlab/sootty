from vcd.reader import *
from ..exceptions import *

import polars as pl

 #key is timestamp, and value is value change
class ValueChange:
    def __init__(self, width=1, data = None):
        self.width = width
        self.data = pl.DataFrame({'time': pl.Series([], dtype=pl.Int64), #Utf8
                                  'value': pl.Series([], dtype=pl.Int64)}) #store both string and integer invers
        
    def add_change(self, time, value): #add a row of time and value for each change
        if value == 'x' or value == 'z':
            value = 0
        else:
            try:
                #try to convert the value to an integer
                value = int(value)
            except ValueError:
                value = 0
        #create a new row 
        new_row = pl.DataFrame({'time': [int(time)], 'value': [int(value)]}) 
        #concatenate new row to existing dataframe
        self.data = pl.concat([self.data, new_row])
        #print("self.data:", self.data)
                               
    def get(self, time):
        result = self.data.filter(pl.col('time') == time) #filter rows that match the time
        if len(result) > 0: #check if there are any matching rows
            return result['value'][0] #this accesses the first column called 'value' and the first element of the series
        else:
            closest_smaller_time = self.data.filter(pl.col('time') < time)['time'].max() #get the smallest max value
            if (closest_smaller_time is not None): #if closestsmallertime exists, filter the dataframe for closestsmallertime and return its value
                closest_value = self.data.filter(pl.col('time') == closest_smaller_time)['value'][0]
                return closest_value
            else: 
                return None

    def length(self):
        if len(self.data) > 0:
            return self.data['time'].max() #finds the lastest time point recorded
        else:
            return 0

    def search(self, function=lambda value: type(value) is int and value > 0, start=None, end=None): #returns list of times that satisfy the function, between start and end times
        #get a filtered dataframe that stores all start to end with function parameter. 
        filtered_data = self.data.filter(function(pl.col('value')))
        if start is not None:
            filtered_data = filtered_data.filter(pl.col('time') >= start)
        if end is not None:
            filtered_data = filtered_data.filter(pl.col('time') <= end)
        return filtered_data['time'].to_list()

    def _to_bool(self): #Im confused by this function
    #Use the `apply` method in Polars to transform each value to a boolean where non-zero and non-null values become 1 and rest become 0
        data = self.data.with_columns(pl.col("value").apply(lambda x: int(bool(x))).alias("value")) # use with_columns method to modify existed value column, alias renames the transfered column back to value
        return ValueChange(width=1, data=data)

    def __invert__(self): #perform a bitwise NOT operation on values in ValueeChange Object
        mask = (2 << self.width - 1) - 1 #bitwise op: shifts the number 2 by self.width - 1; creates a binary number of self.width bits
        data = self.data.with_columns( 
            pl.when(pl.col("value").is_null()) #.when is an if else in polars
            .then(None)
            .otherwise(~pl.col("value") & mask).alias("value") # ~(bitwise not) is applied to every value and then &(bit wise add) is used to ensure results fits
        )
        return ValueChange(width=self.width, data=data)

    def __neg__(self):
        data = self.data.with_columns(
            pl.when(pl.col("value").is_null())
            .then(None)
            .otherwise(-pl.col("value")).alias("value") #-(negat)
        )
        return ValueChange(width=self.width, data=data)

    def __not__(self):
        return not self.width

    def _binop(self, other, binop, width, xz_flag=0): #perform binary operations
        #Check is both self and other dataframes have a columns named value
        if 'value' not in self.data.columns or 'value' not in other.data.columns:
            raise ValueError("Missing 'value' column in one of the DataFrames")
        # Merge `self.data` and `other.data' Dataframes based on time columns. Performs outer join to ensure that all time values are retained
        merged_data = self.data.join(other.data, on='time', how='outer') 
                        
        # Explicitly rename columns to avoid confusion
        merged_data = merged_data.rename({
        'value': 'value_left',
        'value_right': 'value_right'})

        # Define conditions for xz_flag
        if xz_flag == 1:
            condition = (pl.col("value_left") == 0) | (pl.col("value_right") == 0) #logical and
        elif xz_flag == 2:
            condition = (pl.col("value_left") == 1) | (pl.col("value_right") == 1)#logical or
        else:
            condition = pl.lit(True)  # Always True if no xz_flag

        # Apply binary operation,applies binary op to value left and value right columns in merge dataframe based on condition. the result is stored in a new column called value
        data = merged_data.with_columns(
            pl.when(condition)
            .then(binop(pl.col("value_left"), pl.col("value_right")))
            .alias("value")
        )

        return ValueChange(width=width, data=data)

    def __and__(self, other):
        return self._binop(other, lambda x, y: x & y, max(self.width, other.width), 1)

    def __or__(self, other):
        return self._binop(other, lambda x, y: x | y, max(self.width, other.width), 2)

    def __xor__(self, other):
        return self._binop(other, lambda x, y: x ^ y, max(self.width, other.width))

    def __eq__(self, other):
        return self._binop(other, lambda x, y: (x == y).cast(int), 1)

    def __ne__(self, other):
        return self._binop(other, lambda x, y: int(x != y), 1)

    def __gt__(self, other):
        return self._binop(other, lambda x, y: int(x > y), 1)

    def __ge__(self, other):
        return self._binop(other, lambda x, y: int(x >= y), 1)

    def __lt__(self, other):
        return self._binop(other, lambda x, y: int(x < y), 1)

    def __le__(self, other):
        return self._binop(other, lambda x, y: int(x <= y), 1)

    def __lshift__(self, other):
        return self._binop(other, lambda x, y: int(x << y), self.width)

    def __rshift__(self, other):
        return self._binop(other, lambda x, y: int(x >> y), self.width)

    def __add__(self, other):
        return self._binop(other, lambda x, y: x + y, max(self.width, other.width) + 1)

    def __sub__(self, other):
        return self._binop(other, lambda x, y: x - y, max(self.width, other.width) + 1)

    def __mod__(self, other):
        return self._binop(other, lambda x, y: x % y, self.width)

    def _from(self): 

        #important for scenarios where I want the first occurence in time where the value is 1/on
        data = pl.DataFrame({'time': [0], 'value': [0]})
        #iterate through rows
        for row in self.data.iter_rows():
            #is value of that row is true/1
            if row['value']:
                #update data with the first isntance of time with value 1
                data = pl.DataFrame({'time': [row['time']], 'value': [1]})
                break
        return ValueChange(width=1, data=data)

    def _after(self):
        # Filter the dataframe to find the earliest time when 'value' is not zero and select the 'time' column.
        first_true_time = self.data.filter(pl.col('value') != 0).select(pl.col('time')).min()
        # Check if there is a time when 'value' first becomes non-zero.
        if first_true_time['time'][0] is not None:
            # Return a new dataframe with a single row indicating the time just after 'value' becomes non-zero.
            return ValueChange(width=1, data=pl.DataFrame({'time': [first_true_time['time'][0] + 1], 'value': [1]}))
        # Return an empty dataframe if 'value' never becomes non-zero.
        return ValueChange(width=1, data=pl.DataFrame({'time': [], 'value': []}))

    def _until(self):
        # Similar to _after, find the earliest time when 'value' is not zero.
        first_true_time = self.data.filter(pl.col('value') != 0).select(pl.col('time')).min()
        # Check if such a time exists.
        if first_true_time['time'][0] is not None:
            # Return a dataframe with two rows: one at time 0 and one just after 'value' becomes non-zero.
            return ValueChange(width=1, data=pl.DataFrame({'time': [0, first_true_time['time'][0] + 1], 'value': [1, 0]}))
        # Return an empty dataframe if 'value' never becomes non-zero.
        return ValueChange(width=1, data=pl.DataFrame({'time': [], 'value': []}))

    def _before(self):
        # Again, find the earliest time when 'value' is not zero.
        first_true_time = self.data.filter(pl.col('value') != 0).select(pl.col('time')).min()
        # Check if such a time exists.
        if first_true_time['time'][0] is not None:
            # Return a dataframe with two rows: one at time 0 and one just before 'value' becomes non-zero.
            return ValueChange(width=1, data=pl.DataFrame({'time': [0, first_true_time['time'][0]], 'value': [1, 0]}))
        # Return an empty dataframe if 'value' never becomes non-zero.
        return ValueChange(width=1, data=pl.DataFrame({'time': [], 'value': []}))
    
    def _next(self, amt=1):
        # Shift the 'time' values backwards by 'amt', filtering out negative times.
        shifted_data = self.data.with_columns((pl.col('time') - amt).alias('new_time')).filter(pl.col('new_time') >= 0)
        # Return the shifted data with the 'new_time' column renamed back to 'time'.
        return ValueChange(width=self.width, data=shifted_data.rename({'new_time': 'time'}))
    
    def _prev(self, amt=1):
        # Shift the 'time' values forwards by 'amt'.
        shifted_data = self.data.with_columns((pl.col('time') + amt).alias('new_time'))
        # Return the shifted data with the 'new_time' column renamed back to 'time'.
        return ValueChange(width=self.width, data=shifted_data.rename({'new_time': 'time'}))

    def _acc(self):
        



        # Create new columns 'not_null' and 'bool_value' indicating non-null and boolean cast of 'value'.
        data = self.data.with_columns([
            pl.col('value').is_not_null().alias('not_null'),
            pl.col('value').cast(bool).alias('bool_value')
        ])
        # Calculate the difference of 'bool_value' and fill nulls with False.
        #data = data.with_columns(pl.col('bool_value').diff().fill_null(False))
        data = data.with_columns([pl.col('bool_value').cast(int).diff().fill_null(0)])
        # Filter data based on 'data' and 'not_null' columns.
        data = data.filter((pl.col('bool_value') == 1) & (pl.col('not_null') == 1))
        # Calculate the cumulative sum of 'data' and rename it as 'counter'.
        #data = data.with_columns(pl.cum_sum(pl.col('bool_value') == 1).alias('counter'))
        data = data.with_columns([pl.cum_sum(pl.col('bool_value') == 1).alias('counter')])
        # Select only 'time' and 'counter' columns and set width to 0.
        return ValueChange(width=0, data=data.select(['time', 'counter']))


