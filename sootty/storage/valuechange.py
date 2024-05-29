from vcd.reader import *
from itertools import islice
from sortedcontainers import SortedDict, SortedList, SortedSet

from ..exceptions import *

class ValueChange(SortedDict):
    def __init__(self, width=1, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.width = width

    def get(self, key):
        if key in self:
            return self[key]
        if len(self) < 1 or key < next(self.irange()):
            return None
        return self[next(islice(self.irange(maximum=key, reverse=True), 1))]

    def length(self):
        """Returns the time duration of the wire."""
        return next(self.irange(reverse=True)) if len(self) > 0 else 0

    def search(
        self,
        function=lambda value: type(value) is int and value > 0,
        start=None,
        end=None,
    ):
        """Returns a list of times that satisfy the function, between start and end times."""
        indices = []
        prev = None
        for i in self.irange(minimum=start, maximum=end):
            if prev is not None:
                indices.extend(range(prev + 1, i))
            if function(self[i]):
                indices.append(i)
                prev = i
            else:
                prev = None
        if prev is not None and end is not None and end > prev:
            indices.extend(range(prev + 1, end))
        return indices

    def _to_bool(self):
        data = ValueChange(width=1)
        for key in self:
            data[key] = None if self[key] == None else (int(bool(self[key])))
        return data

    def __invert__(self):
        data = ValueChange(width=self.width)
        for key in self:
            data[key] = (
                None if self[key] == None else (~self[key] & (2 << self.width - 1) - 1)
            )
        return data

    def __neg__(self):
        data = ValueChange(width=self.width)
        for key in self:
            data[key] = None if self[key] == None else (-self[key])
        return data

    def __not__(self):
        return not (self.width)

    def _binop(self, other, binop, width, xz_flag=0):
        data = ValueChange(width=width) 
        keys = SortedSet()
        keys.update(self.keys())
        keys.update(other.keys())
        values = [None, None, None]

        for key in keys:
            reduced = None
            if key in self:
                values[0] = self[key]
            if key in other:
                values[1] = other[key]
            if xz_flag == 1:
                if values[0] == 0 or values[1] == 0:  # xz = 1 is logical and
                    reduced = 0
            if xz_flag == 2:  # xz = 2 is logical or
                if values[0] == 1 or values[1] == 1:
                    reduced = 1
            if reduced is None:
                reduced = (
                    None
                    if (
                        (values[0] is None or values[1] is None)
                        or (type(values[0]) == str)
                        or (type(values[1]) == str)
                    )
                    else binop(values[0], values[1])
                )
            if reduced != values[2]:
                values[2] = reduced
                data[key] = reduced
        return data

    def __and__(self, other):
        return self._binop(other, lambda x, y: x & y, max(self.width, other.width), 1)

    def __or__(self, other):
        return self._binop(other, lambda x, y: x | y, max(self.width, other.width), 2)

    def __xor__(self, other):
        return self._binop(other, lambda x, y: x ^ y, max(self.width, other.width))

    def __eq__(self, other):
        return self._binop(other, lambda x, y: int(x == y), 1)

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
        data = ValueChange(width=1)
        data[0] = 0
        for key in self:
            if self[key]:
                data[key] = 1
                break
        return data

    def _after(self):
        data = ValueChange(width=1)
        data[0] = 0
        for key in self:
            if self[key]:
                data[key + 1] = 1
                break
        return data

    def _until(self):
        data = ValueChange(width=1)
        data[0] = 1
        for key in self:
            if self[key]:
                data[key + 1] = 0
                break
        return data

    def _before(self):
        data = ValueChange(width=1)
        data[0] = 1
        for key in self:
            if self[key]:
                data[key] = 0
                break
        return data

    def _next(self, amt=1):
        data = ValueChange(width=self.width)
        data[0] = self.get(amt)
        for key in self.irange(minimum=amt):
            data[key - 1] = self[key]
        return data

    def _prev(self, amt=1):
        data = ValueChange(width=self.width)
        for key in self:
            data[key + 1] = self[key]
        return data

    def _acc(self):
        data = ValueChange(width=0)
        counter = 0
        data[0] = counter
        state = True
        for key in self:
            if self[key] and not state:
                state = True
                counter += 1
                data[key] = counter
            elif not self[key] and state:
                state = False
        return data

# from vcd.reader import *
# from ..exceptions import *

# import polars as pl

#  #key is timestamp, and value is value change
# class ValueChange:
#     def __init__(self, width=1, data = None):
#         self.width = width
#         self.data = pl.DataFrame({'time': pl.Series([], dtype=pl.Int64), 
#                                   'value': pl.Series([], dtype=pl.Int64)})
#         self.pending_data = []
        
#     def add_change(self, time, value): #add a row of time and value for each change
#         if value == 'x' or value == 'z':
#             value = 0
#         else:
#             try:
#                 #try to convert the value to an integer
#                 value = int(value)
#             except ValueError:
#                 value = 0
#         #create a new row 
#         new_row = pl.DataFrame({'time': [time], 'value': [value]}) 
#         #concatenate new row to existing dataframe
#         self.data = pl.concat([self.data, new_row])
#         print("self.data:", self.data)     
#     # def apply_datachanges(self):
#     #     if self.pending_data:
#     #         #created df from pending data
#     #         new_data = pl.DataFrame(self.pending_data)
#     #         #concat self.data to new_data
#     #         self.data = pl.concat([self.data,new_data])
#     #         #self.pending_data.clear()
                               
#     def get(self, time):
#         result = self.data.filter(pl.col('time') == time) #filter rows that match the time
#         assert isinstance(result, pl.DataFrame), "Result is not a Polars DataFrame"
#         print("result:", result)
#         if len(result) > 0: #check if there are any matching rows
#             #print("This the result:", result['value'][0])
#             return result['value'][0] #this accesses the first column called 'value' and the first element of the series
#         else:
#             closest_smaller_time = self.data.filter(pl.col('time') < time)['time'].max() #get the smallest max value
#             if (closest_smaller_time is not None): #if closestsmallertime exists, filter the dataframe for closestsmallertime and return its value
#                 closest_value = self.data.filter(pl.col('time') == closest_smaller_time)['value'][0]
#                 return closest_value
#             else: 
#                 return None

#     def length(self):
#         if len(self.data) > 0:
#             return self.data['time'].max() #finds the lastest time point recorded
#         else:
#             return 0

#     def search(self, function=lambda value: type(value) is int and value > 0, start=None, end=None): #returns list of times that satisfy the function, between start and end times
#         #get a filtered dataframe that stores all start to end with function parameter. 
#         filtered_data = self.data.filter(function(pl.col('value')))
#         if start is not None:
#             filtered_data = filtered_data.filter(pl.col('time') >= start)
#         if end is not None:
#             filtered_data = filtered_data.filter(pl.col('time') <= end)
#         #print(filtered_data)
#         return filtered_data['time'].to_list()

#     # def _to_bool(self): #Im confused by this function
#     # #Use the `apply` method in Polars to transform each value to a boolean where non-zero and non-null values become 1 and rest become 0
#     #     data = self.data.with_columns(pl.col("value").apply(lambda x: int(bool(x))).alias("value")) # use with_columns method to modify existed value column, alias renames the transfered column back to value
#     #     return ValueChange(width=1, data=data)

#     # def __invert__(self): #perform a bitwise NOT operation on values in ValueeChange Object
#     #     mask = (2 << self.width - 1) - 1 #bitwise op: shifts the number 2 by self.width - 1; creates a binary number of self.width bits
#     #     data = self.data.with_columns( 
#     #         pl.when(pl.col("value").is_null()) #.when is an if else in polars
#     #         .then(None)
#     #         .otherwise(~pl.col("value") & mask).alias("value") # ~(bitwise not) is applied to every value and then &(bit wise add) is used to ensure results fits
#     #     )
#     #     return ValueChange(width=self.width, data=data)

#     # def __neg__(self):
#     #     data = self.data.with_columns(
#     #         pl.when(pl.col("value").is_null())
#     #         .then(None)
#     #         .otherwise(-pl.col("value")).alias("value") #-(negat)
#     #     )
#     #     return ValueChange(width=self.width, data=data)

#     # def __not__(self):
#     #     return not self.width

#     # def _binop(self, other, binop, width, xz_flag=0): #perform binary operations
#     #     #Check is both self and other dataframes have a columns named value
#     #     if 'value' not in self.data.columns or 'value' not in other.data.columns:
#     #         raise ValueError("Missing 'value' column in one of the DataFrames")
#     #     # Merge `self.data` and `other.data' Dataframes based on time columns. Performs outer join to ensure that all time values are retained
#     #     merged_data = self.data.join(other.data, on='time', how='outer') 
                        
#     #     # Explicitly rename columns to avoid confusion
#     #     merged_data = merged_data.rename({
#     #     'value': 'value_left',
#     #     'value_right': 'value_right'})

#     #     # Define conditions for xz_flag
#     #     if xz_flag == 1:
#     #         condition = (pl.col("value_left") == 0) | (pl.col("value_right") == 0) #logical and
#     #     elif xz_flag == 2:
#     #         condition = (pl.col("value_left") == 1) | (pl.col("value_right") == 1)#logical or
#     #     else:
#     #         condition = pl.lit(True)  # Always True if no xz_flag

#     #     # Apply binary operation,applies binary op to value left and value right columns in merge dataframe based on condition. the result is stored in a new column called value
#     #     data = merged_data.with_columns(
#     #         pl.when(condition)
#     #         .then(binop(pl.col("value_left"), pl.col("value_right")))
#     #         .alias("value")
#     #     )

#     #     return ValueChange(width=width, data=data)