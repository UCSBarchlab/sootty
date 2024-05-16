from itertools import compress, chain

from ..exceptions import *
from .valuechange import ValueChange
from sortedcontainers import SortedDict, SortedList, SortedSet
import polars as pl
# import time

def flip_each_bit(value, width):
    return int(~(value)) & (2 << width - 1) - 1

def to_bool_helper(value):
    if(value > 0):
        return 1
    else:
        return 0

class Wire:
    def __init__(self, name, width=1):
        self.name = name
        self.bit_width = width
        self.init_val = 0
        # self._data = ValueChange(width)
        self._data_df = pl.DataFrame(schema={"time": pl.Int64, "value": pl.Int64})

    # Used to get data from pyrtl - Not implementing yet
    @classmethod
    def from_data(cls, name, data, width=1):
        cls.bit_width = width
        wire = cls(name=name, width=width)
        for key in compress(
            range(len(data)),
            map(lambda pair: pair[0] != pair[1], zip(chain([None], data), data)), #zip combines multiple interables into tuples, chain combined multiple iterables into single interable, 
        ):
            wire[key] = data[key]
        return wire

    # Add value change to wire's df
    def __setitem__(self, key, value):
        if(key == 0):
            # self._data[key] = value
            self.init_val = value
        else:
            # self._data[key] = value
            temp_vc = pl.DataFrame({'time': [int(key)], 'value': [int(value)]})
            self._data_df = pl.concat(
                    [
                        self._data_df, 
                        temp_vc
                    ],
                    how="vertical")

    # Gets value of wire at time (key)
    def __getitem__(self, key):
        # filtered = self._data_df.filter(pl.col("time") <= key)
        # height = filtered.height
        # if(height > 0):
        #     return (filtered[height-1].select(pl.col("value")).item())
        # else:
        #     return self.init_val
        time_column = self._data_df.get_column("time")
        length = time_column.len()
        
        if length == 0:
            return self.init_val
        
        key_idx = time_column.search_sorted(key)

        if (length-1) < key_idx:
            key_idx = length - 1

        time = self._data_df.get_column("time")[key_idx]

        if time != key:
            if key_idx == 0:
                return self.init_val
            else:
                return self._data_df.get_column("value")[key_idx - 1]
        else:
            return self._data_df.get_column("value")[key_idx]

    # Not called TODO: Test this
    def __delitem__(self, key):
        # del self._data[key]  # throws error if not present
        self._data_df.drop(key)

    # TODO: Not sure why this is called, hardcoding to 1 for now - maybe bitwidth
    def width(self):
        return self.bit_width
        # return self._data.width

    def length(self):
        """Returns the time duration of the wire."""
        #AKA: returns last time change
        height = self._data_df.height
        if(height > 0):
            return (self._data_df[height-1].select(pl.col("time")).item())
        else:
            return 0

    # Not called TODO: Test this
    def end(self):
        """Returns the final value on the wire"""
        height = self._data_df.height
        if(height > 0):
            return (self._data_df[height-1].select(pl.col("value")).item())
        else:
            return 0
        # return self._data[self._data.length()]

    # TODO: test this with returns that are more than one value (not just [20], instead like [20, 22])
    def times(self, length=0):
        """Returns a list of times with high value on the wire."""
        value = []
        if(self.init_val == 1):
            value = [0]

        value += self._data_df.filter(pl.col("value") > 0).get_column("time").to_list()

        # print("Wire:", self.name, "value:", value)
        # if len(value) > 0:
        #     # cast the tuple to a list
        #     return value
        # else:
        return value
        # return self._data.search(end=max(length, self.length()))

    @classmethod
    def const(cls, value):
        wire = cls(name=f"c_{value}", width=0)
        wire[0] = value
        wire.init_val = value
        return wire

    @classmethod
    def time(cls, value):
        wire = cls(name=f"t_{value}", width=1)

        #NEW
        wire._data_df = pl.DataFrame({'time': [0, int(value), int(value + 1)], 'value': [0, 1, 0]})
        
        #OLD TODO: Delete (when we delete self._data)
        wire[0] = 0
        wire[value] = 1
        wire[value + 1] = 0

        return wire


    ## UPDATED SUCCESSFULLY
    def __invert__(self):
        wire = Wire(name="~" + self.name)
        wire.bit_width = self.bit_width
        wire._data_df = self._data_df.with_columns((pl.col("value").apply(lambda x: flip_each_bit(x, wire.bit_width), return_dtype= pl.UInt64)))

        # wire._data = self._data.__invert__() # Old, TODO: delete later
        return wire

    def __neg__(self):
        wire = Wire(name="-" + self.name)
        # wire._data = self._data.__invert__()
        return wire

    ## UPDATED SUCCESSFULLY
    def __and__(self, other):
        wire = Wire(name="(" + self.name + " & " + other.name + ")")
        # wire._data = self._data.__and__(other._data)
        wire._binop(self, other, lambda x, y: x & y, max(self.bit_width, other.bit_width), 1)
        return wire

    ## UPDATED SUCCESSFULLY
    def __or__(self, other):
        wire = Wire(name="(" + self.name + " | " + other.name + ")")
        # wire._data = self._data.__or__(other._data)
        wire._binop(self, other, lambda x, y: x | y, max(self.bit_width, other.bit_width), 2)
        return wire

    ## UPDATED SUCCESSFULLY
    def __xor__(self, other):
        wire = Wire(name="(" + self.name + " ^ " + other.name + ")")
        # wire._data = self._data.__xor__(other._data)
        wire._binop(self, other, lambda x, y: x ^ y, max(self.bit_width, other.bit_width))
        return wire
    
    #Change(from valuechange.py)
    def _to_bool(self):
        self.init_val = to_bool_helper(self.init_val)
        self._data_df = self._data_df.with_columns((pl.col("value").apply(lambda x: to_bool_helper(x), return_dtype= pl.Int64)))


    #! not working on all terminals? TODO: ask Balkind
    def _logical_not(self):
        wire = Wire(name="!" + self.name)
        # wire._data = self._data._to_bool().__invert__()
        return wire

    ## UPDATED SUCCESSFULLY
    def _logical_and(self, other):
        wire = Wire(name="(" + self.name + " && " + other.name + ")")
        wire.init_val = self.init_val
        wire._data_df = self._data_df
        wire.bit_width = self.bit_width

        # Add temporary copy of other
        temp_wire = Wire(name=other.name)
        temp_wire.init_val = other.init_val
        temp_wire._data_df = other._data_df
        temp_wire.bit_width = self.bit_width

        # New Query
        wire._to_bool()
        temp_wire._to_bool()
        wire = wire.__and__(temp_wire)


        # Old Query TODO: delete later
        # wire._data = self._data._to_bool().__and__(other._data._to_bool())
        return wire

    ## UPDATED SUCCESSFULLY
    def _logical_or(self, other):
        wire = Wire(name="(" + self.name + " || " + other.name + ")")
        wire.init_val = self.init_val
        wire._data_df = self._data_df
        wire.bit_width = self.bit_width

        # Add temporary copy of other
        temp_wire = Wire(name=other.name)
        temp_wire.init_val = other.init_val
        temp_wire._data_df = other._data_df
        temp_wire.bit_width = self.bit_width

        # New Query
        # TODO: make to_bool work for xxxxxx values
        wire._to_bool()
        temp_wire._to_bool()
        wire = wire.__or__(temp_wire)

        # Old Query TODO: delete later
        # wire._data = self._data._to_bool().__or__(other._data._to_bool())
        return wire

    ## UPDATED SUCCESSFULLY
    def __eq__(self, other):
        wire = Wire(name="(" + self.name + " == " + other.name + ")")
        wire._binop(self, other, lambda x, y: int(x == y), 1) # passing in self to "first" parameter in binop
        return wire
 
    ## UPDATED SUCCESSFULLY
    def __ne__(self, other):
        wire = Wire(name="(" + self.name + " != " + other.name + ")")
        # wire._data = self._data.__ne__(other._data)
        wire._binop(self, other, lambda x, y: int(x != y), 1)
        return wire

    ## UPDATED SUCCESSFULLY
    def __gt__(self, other):
        wire = Wire(name="(" + self.name + " > " + other.name + ")")
        # wire._data = self._data.__gt__(other._data)
        wire._binop(self, other, lambda x, y: int(x > y), 1)
        return wire
    
    ## UPDATED SUCCESSFULLY
    def __ge__(self, other):
        wire = Wire(name="(" + self.name + " >= " + other.name + ")")
        # wire._data = self._data.__ge__(other._data)
        wire._binop(self, other, lambda x, y: int(x >= y), 1)
        return wire

    ## UPDATED SUCCESSFULLY
    def __lt__(self, other):
        wire = Wire(name="(" + self.name + " < " + other.name + ")")
        # wire._data = self._data.__lt__(other._data)
        wire._binop(self, other, lambda x, y: int(x < y), 1)
        return wire

    ## UPDATED SUCCESSFULLY
    def __le__(self, other):
        wire = Wire(name="(" + self.name + " <= " + other.name + ")")
        # wire._data = self._data.__le__(other._data)
        wire._binop(self, other, lambda x, y: int(x <= y), 1)
        return wire

    ## UPDATED SUCCESSFULLY
    # Example Query: sootty "example/example1.vcd" -l 8 -s "Data << const 2 == const 1" -w "D1,D0,Data"
    def __lshift__(self, other):
        wire = Wire(name="(" + self.name + " << " + other.name + ")")
        # wire._data = self._data.__lshift__(other._data)
        wire._binop(self, other, lambda x, y: int(x << y), self.bit_width)
        return wire
    
    ## UPDATED SUCCESSFULLY
    def __rshift__(self, other):
        wire = Wire(name="(" + self.name + " >> " + other.name + ")")
        # wire._data = self._data.__rshift__(other._data)
        wire._binop(self, other, lambda x, y: int(x >> y), self.bit_width)
        return wire
    
    ## UPDATED SUCCESSFULLY
    def __add__(self, other):
        wire = Wire(name="(" + self.name + " + " + other.name + ")")
        # wire._data = self._data.__add__(other._data)
        # print("self width: ")
        wire._binop(self, other, lambda x, y: x + y, max(self.bit_width, other.bit_width) + 1)
        return wire

    ## UPDATED SUCCESSFULLY
    def __sub__(self, other):
        wire = Wire(name="(" + self.name + " - " + other.name + ")")
        # wire._data = self._data.__sub__(other._data)
        wire._binop(self, other, lambda x, y: x - y, max(self.bit_width, other.bit_width) + 1)
        return wire

    ## UPDATED SUCCESSFULLY
    def __mod__(self, other):
        wire = Wire(name="(" + self.name + " % " + other.name + ")")
        # wire._data = self._data.__mod__(other._data)
        wire._binop(self, other, lambda x, y: x % y, self.bit_width)
        return wire

######### NOT IMPLEMENTING - Start #########
    def _from(self):
        wire = Wire(name="from " + self.name)
        # wire._data = self._data._from()
        return wire

    def _after(self):
        wire = Wire(name="after " + self.name)
        # wire._data = self._data._after()
        return wire

    def _until(self):
        wire = Wire(name="until " + self.name)
        # wire._data = self._data._until()
        return wire

    def _before(self):
        wire = Wire(name="before " + self.name)
        # wire._data = self._data._before()
        return wire

    def _next(self, amt=1):
        wire = Wire(name="next " + self.name)
        # wire._data = self._data._next(amt)
        return wire

    def _prev(self, amt=1):
        wire = Wire(name="prev " + self.name)
        # wire._data = self._data._prev(amt)
        return wire

    def _acc(self):
        wire = Wire(name="acc " + self.name)
        # wire._data = self._data._acc()
        return wire
    ######### NOT IMPLEMENTING - End #########

    ## UPDATED SUCCESSFULLY
    def get_all_times(self):
        return self._data_df.get_column("time").to_list()
    
    ## UPDATED SUCCESSFULLY
    def change_at_time(self, key):
        # check if key exists in dataframe
        # if key == 0:
        #     return True
        # filtered = self._data_df.filter(pl.col("time") == key)
        # height = filtered.height
        # if(height > 0):
        #     return True
        # else:
        #     return False
        if key == 0:
            return True

        time_column = self._data_df.get_column("time")
        length = time_column.len()
        
        if length == 0:
            return self.init_val
        
        key_idx = time_column.search_sorted(key)

        if (length-1) < key_idx:
            key_idx = length - 1

        time = self._data_df.get_column("time")[key_idx]

        if time != key:
            return False
        else:
            return True

    ## UPDATED SUCCESSFULLY
    def _binop(self, first, other, binop, width, xz_flag=0): 
            # self._data = ValueChange(width=width)
            self.bit_width = width
            keys = SortedSet()
            keys.update(first.get_all_times())
            keys.update(other.get_all_times())
            # Add 0th index if applicable here
            if (isinstance(first.init_val, int)) and (isinstance(other.init_val, int)):
                keys.update([0])

            values = [None, None, None]

            for key in keys:
                reduced = None
                if first.change_at_time(key):
                    values[0] = first.__getitem__(key)
                    #print("value")
                    # values[0] = first[key]
                if other.change_at_time(key):
                    values[1] = other.__getitem__(key)
                    # values[1] = other[key]
                if xz_flag == 1:
                    # print("xz_flag:", xz_flag)
                    if values[0] == 0 or values[1] == 0:  # xz = 1 is logical and
                        reduced = 0
                if xz_flag == 2:  # xz = 2 is logical or
                    # print("xz_flag:", xz_flag)
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
                # print("key:", key, "first wire:", values[0], "other wire:", values[1], "values[2]: ", values[2], "reduced:",reduced)
                if reduced != values[2]:
                    values[2] = reduced
                    # self._data[key] = reduced
                    # print("adding key to df: ", key)
                    self.__setitem__(key, reduced)
                    # print("Data frame after adding:", self._data_df.collect())
                # if reduced == 1 and values[2] == 0:
                #     values[2] = reduced
                #     self._data[key] = reduced
                #     self.__setitem__(key, 1)
                # if reduced == 0:
                #     values[2] = reduced
            #     print("Values[", key, "]: ",values)
            # print("return data:",self._data)
            # print("return data:",self._data_df.collect())
            # print("return data init_val:", self.init_val)
