from distutils.core import setup
from Cython.Build import cythonize

setup(ext_modules = cythonize(
        r'c:\Users\sottilep\PycharmProjects\VentDysSedation\AnalysisModules\WaveformAnalysis.py'), requires = ['pandas',
                                                                                                               'numpy',
                                                                                                               'scipy',
                                                                                                               'bokeh',
                                                                                                               'ipyparallel',
                                                                                                               'ggplot',
                                                                                                               'numba',
                                                                                                               'numba',
                                                                                                               'numba',
                                                                                                               'Cython'])
