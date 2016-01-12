from distutils.core import setup
from Cython.Build import cythonize

setup(ext_modules = cythonize(
      r'c:\Users\sottilep\PycharmProjects\VentDysSedation\CreationModules\DatabaseCreation.py'))
