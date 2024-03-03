from itertools import compress, chain

from ..exceptions import *
from .valuechange import ValueChange
from sortedcontainers import SortedDict, SortedList, SortedSet
import polars as pl


class Wire:
    def __init__(self, name, width=1):
        self.name = name
        self.bit_width = width
        self.init_val = 0
        self._data = ValueChange(width)
        self._data_df = pl.LazyFrame(schema={"time": pl.Int64, "value": pl.Int64})

    # Used to get data from pyrtl - Not implementing yet
    @classmethod
    def from_data(cls, name, data, width=1):
        cls.bit_width = width
        wire = cls(name=name, width=width)
        for key in compress(
            range(len(data)),
            map(lambda pair: pair[0] != pair[1], zip(chain([None], data), data)),
        ):
            wire[key] = data[key]
        return wire

    # Add value change to wire's df
    def __setitem__(self, key, value):
        if(key == 0):
            self.init_val = value
        else:
            self._data[key] = value
            temp_vc = pl.LazyFrame({'time': [int(key)], 'value': [int(value)]})
            self._data_df = pl.concat(
                    [
                        self._data_df, 
                        temp_vc
                    ],
                    how="vertical")

    # Gets value of wire at time (key)
    def __getitem__(self, key):
        value = self._data_df.filter(pl.col("time") <= key).last().select(pl.col("value")).collect()
        if value.is_empty():
            return self.init_val
        else:
            return value.item()
        # return self._data.get(key)

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
        # return self._data.length()
        value = self._data_df.last().collect().select(pl.col("time"))
        if value.is_empty():
            return 0
        else:
            return value.item()

    # Not called TODO: Test this
    def end(self):
        """Returns the final value on the wire"""
        value = self._data_df.last().collect().select(pl.col("value"))
        if value.is_empty():
            return self.init_val
        else:
            return value.item()

    # TODO: test this with returns that are more than one value (not just [20], instead like [20, 22])
    def times(self, length=0):
        """Returns a list of times with high value on the wire."""
        value = self._data_df.filter(pl.col("value") > 0).collect().get_column("time").to_list()
        print("value", value)
        # if len(value) > 0:
        #     # cast the tuple to a list
        #     return value
        # else:
        return value
        # return self._data.search(end=max(length, self.length()))

    # TODO: Not implemented
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
        wire._data_df = pl.LazyFrame({'time': [0, int(value), int(value + 1)], 'value': [0, 1, 0]})
        
        #OLD TODO: Delete (when we delete self._data)
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
        print("wire self:", self.name, self.init_val, self._data_df.collect(), "wire other:", other.name, other.init_val, other._data_df.collect())
        wire = Wire(name="(" + self.name + " == " + other.name + ")")

        # # self._binop(other, lambda x, y: int(x == y), 1)
        # self._binop(self, other, lambda x, y: int(x == y), 1)
        
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
    
    def get_all_times(self):
        return self._data_df.collect().get_column("time").to_list()

    def _binop(self, other, binop, width, xz_flag=0):
            data = ValueChange(width=width) 
            keys = SortedSet()
            keys.update(self.get_all_times())
            keys.update(other.get_all_times())
            values = [None, None, None]
            print("data:", data)
            print("keys:", keys)
            print("values:", values)
            print("self:", self)
            print("other:", other)
            print("binop:", binop)
            print("width:", width)
            print("xz_flag:", xz_flag)

            for key in keys:
                reduced = None
                if key in self._data_df:
                    values[0] = self[key]
                if key in other._data_df:
                    values[1] = other[key]
                if xz_flag == 1:
                    if values[0] == 0 or values[1] == 0:  # xz = 1 is logical and
                        reduced = 0
                if xz_flag == 2:  # xz = 2 is logical or
                    if values[0] == 1 or values[1] == 1:
                        reduced = 1
                print("reduced:",reduced)
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
                print("Values[", key, "]: ",values)
            print("return data:",data)
            return data