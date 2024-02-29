from sootty.storage import Wire

import unittest

class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

if __name__ == '__main__':
    unittest.main()
    #This block ensures that the tests are executed only when the script is run directly, not when it's imported as a module.
