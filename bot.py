from ctypes import *

parser = cdll.LoadLibrary("parser.so")
parser.ParserInit.restype = None
parser.ParserInit(4, 100, 10)
