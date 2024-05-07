from itertools import compress, chain

from ..exceptions import *
from .valuechange import ValueChange
import polars as pl


class Wire:
    def __init__(self, name, width=1):
        self.name = name
        self._data = ValueChange()
        self.bit_width = width
        # column-based dataframe of value changes
        self._data_df = pl.DataFrame()
        self.init_val = 0

    # Used to get data from pyrtl - Not implementing yet
    @classmethod
    def from_data(cls, name, data, width=1):
        wire = cls(name=name, width=width)
        for key in compress(
            range(len(data)),
            map(lambda pair: pair[0] != pair[1], zip(chain([None], data), data)),
        ):
            wire[key] = data[key]
        return wire

    ### DONE ###
    # Add value change to wire's df
    def __setitem__(self, key, value):
        self._data[key] = value #Todo delete

        if key == 0:
            self.init_val = value
        else:
            temp_vc = pl.DataFrame({str(key): [int(value)]})
            self._data_df = pl.concat(
                    [
                        self._data_df, 
                        temp_vc
                    ],
                    how="horizontal")
        
    
    # Gets value of wire at time (key)
    def __getitem__(self, key):
        df_width = self._data_df.width
        names = self._data_df.columns

        # Gets value for init value and any value
        # before the first key in dataframe
        if(key < int(names[0])):
            return self.init_val

        
        # Perform binary search
        low = 0
        high = df_width
        result = -1
        
        while (high - low) > 1:
            half_width = (low + high) // 2
            half_width_val = names[half_width]
            if key == int(half_width_val):
                result = half_width
                break
            elif key < int(half_width_val):
                high = half_width
            elif key > int(half_width_val):
                low = half_width

        # Doesn't find key
        if (high-low <= 1):
            result = low

        result = self._data_df.to_series(result)[0]
        return result
    
    # Not called TODO: Test this
    def __delitem__(self, key):
        del self._data[key]  # throws error if not present
        # self._data_df.drop(key)

    # TODO: Not sure why this is called, hardcoding to 1 for now - maybe bitwidth
    def width(self):
        # return self._data.width
        return self.bit_width

    def length(self):
        """Returns the time duration of the wire."""
        #AKA: returns last time change
        width = self._data_df.width
        times = self._data_df.columns
        if(width == 0):
            #empty df, only init_val
            return 0
        else:
            #has last time in df
            return int(times[width-1])
    
    # Not called TODO: Test this
    def end(self):
        """Returns the final value on the wire"""
        last_key = self.length()
        if last_key == 0:
            return self.init_val
        else:
            return self._data_df.to_series()[0]


    # TODO: maybe just make a column with all the times that are on
    def times(self, length=0):
        """Returns a list of times with high value on the wire."""
        print("CALLED on ", self.name, self._data_df)

        if self._data_df.width == 0:
            if self.init_val == 1:
                return [0]
            else:
                return []

        times = self._data_df.row(0)
        print(times)
        return self._data.search(end=max(length, self.length()))

        value = []
        if(self.init_val == 1):
            value = [0]

        value += self._data_df.filter(pl.col("value") > 0).collect().get_column("time").to_list()
        return value

    @classmethod
    def const(cls, value):
        wire = cls(name=f"c_{value}", width=0)
        wire[0] = value
        return wire

    @classmethod
    def time(cls, value):
        wire = cls(name=f"t_{value}", width=1)
        wire[0] = 0
        wire[value] = 1
        wire[value + 1] = 0
        return wire

    def __invert__(self):
        wire = Wire(name="~" + self.name)
        wire._data = self._data.__invert__()
        return wire

    def __neg__(self):
        wire = Wire(name="-" + self.name)
        wire._data = self._data.__invert__()
        return wire

    def __and__(self, other):
        wire = Wire(name="(" + self.name + " & " + other.name + ")")
        wire._data = self._data.__and__(other._data)
        return wire

    def __or__(self, other):
        wire = Wire(name="(" + self.name + " | " + other.name + ")")
        wire._data = self._data.__or__(other._data)
        return wire

    def __xor__(self, other):
        wire = Wire(name="(" + self.name + " ^ " + other.name + ")")
        wire._data = self._data.__xor__(other._data)
        return wire

    def _logical_not(self):
        wire = Wire(name="!" + self.name)
        wire._data = self._data._to_bool().__invert__()
        return wire

    def _logical_and(self, other):
        wire = Wire(name="(" + self.name + " && " + other.name + ")")
        wire._data = self._data._to_bool().__and__(other._data._to_bool())
        return wire

    def _logical_or(self, other):
        wire = Wire(name="(" + self.name + " || " + other.name + ")")
        wire._data = self._data._to_bool().__or__(other._data._to_bool())
        return wire

    def __eq__(self, other):
        wire = Wire(name="(" + self.name + " == " + other.name + ")")
        wire._data = self._data.__eq__(other._data)
        return wire

    def __ne__(self, other):
        wire = Wire(name="(" + self.name + " != " + other.name + ")")
        wire._data = self._data.__ne__(other._data)
        return wire

    def __gt__(self, other):
        wire = Wire(name="(" + self.name + " > " + other.name + ")")
        wire._data = self._data.__gt__(other._data)
        return wire

    def __ge__(self, other):
        wire = Wire(name="(" + self.name + " >= " + other.name + ")")
        wire._data = self._data.__ge__(other._data)
        return wire

    def __lt__(self, other):
        wire = Wire(name="(" + self.name + " < " + other.name + ")")
        wire._data = self._data.__lt__(other._data)
        return wire

    def __le__(self, other):
        wire = Wire(name="(" + self.name + " <= " + other.name + ")")
        wire._data = self._data.__le__(other._data)
        return wire

    def __lshift__(self, other):
        wire = Wire(name="(" + self.name + " << " + other.name + ")")
        wire._data = self._data.__lshift__(other._data)
        return wire

    def __rshift__(self, other):
        wire = Wire(name="(" + self.name + " >> " + other.name + ")")
        wire._data = self._data.__rshift__(other._data)
        return wire

    def __add__(self, other):
        wire = Wire(name="(" + self.name + " + " + other.name + ")")
        wire._data = self._data.__add__(other._data)
        return wire

    def __sub__(self, other):
        wire = Wire(name="(" + self.name + " - " + other.name + ")")
        wire._data = self._data.__sub__(other._data)
        return wire

    def __mod__(self, other):
        wire = Wire(name="(" + self.name + " % " + other.name + ")")
        wire._data = self._data.__mod__(other._data)
        return wire

    def _from(self):
        wire = Wire(name="from " + self.name)
        wire._data = self._data._from()
        return wire

    def _after(self):
        wire = Wire(name="after " + self.name)
        wire._data = self._data._after()
        return wire

    def _until(self):
        wire = Wire(name="until " + self.name)
        wire._data = self._data._until()
        return wire

    def _before(self):
        wire = Wire(name="before " + self.name)
        wire._data = self._data._before()
        return wire

    def _next(self, amt=1):
        wire = Wire(name="next " + self.name)
        wire._data = self._data._next(amt)
        return wire

    def _prev(self, amt=1):
        wire = Wire(name="prev " + self.name)
        wire._data = self._data._prev(amt)
        return wire

    def _acc(self):
        wire = Wire(name="acc " + self.name)
        wire._data = self._data._acc()
        return wire
