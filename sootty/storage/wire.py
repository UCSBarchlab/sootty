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
            print("key, value:", )
            self._data_df = pl.concat(
                    [
                        self._data_df, 
                        temp_vc
                    ],
                    how="horizontal")
        
    
    # Gets value of wire at time (key)
    def __getitem__(self, key):
        print("Name: ", self.name, "Key: ", key)
        print("Unfiltered:", self._data_df)

        if (key == 0):
            return self.init_val


        df_width = self._data_df.width
        
        names = self._data_df.columns
        print("names",names)
        

        low = 0
        high = df_width
        half_width = high//2
        while low <= high:
            half_width_val = names[half_width]
            if key == half_width_val:
                #to_series
                print("FOUND COLUMN")
                break
            elif key < half_width_val:
                half_width = (half_width) // 2
            elif key > half_width_val:
                half_width = (df_width - half_width) // 2
                print("half width value", names[half_width])


        #OLD
        if (key == 0):
            return self.init_val

        latest_column = pl.Series()
        for col in self._data_df:
            if int(col.name) <= key:
                latest_column = col
        if(latest_column.len() > 0):
            return latest_column.item()
            
        
        # print ("old value: ",self._data.get(key))
        # return self._data.get(key)

        # return self._data_df.get_column(str(key))[0]['value']

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
        return self._data.length()
        return int(self._data_df.select(pl.last()).columns[0])

    # Not called TODO: Test this
    def end(self):
        """Returns the final value on the wire"""
        return int(self._data_df.select(pl.last())[0])

    # TODO: maybe just make a column with all the times that are on
    def times(self, length=0):
        """Returns a list of times with high value on the wire."""
        return self._data.search(end=max(length, self.length()))
        
        rtn_val_list = []

        # Works as well as line below but not sure if is faster or not TODO: Test against line below
        for col in self._data_df.select(pl.all()):
            # print(col[0]['value'])
            if col[0] == 1:
                rtn_val_list.append(int(col.name))

        # Works as well but not sure if is faster or not TODO: Test against line above
        # rtn_val_list = [ int(col.name) for col in self._data_df.select(pl.all() == 1) if col.all() ]
        return rtn_val_list

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
