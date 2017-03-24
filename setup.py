from distutils.core import setup


setup(name='onegeo-manager',
      version='0.0.1',
      description='',
      long_description='',
      author='Neogeo Technologies',
      author_email='contact@neogeo.fr',
      url='https://github.com/neogeo-technologies/onegeo-manager',
      license="GPLv3",
      classifiers=[
              'Development Status :: Beta',
              'Intended Audience :: Developers',
              'Intended Audience :: Science/Research',
              'License :: OSI Approved :: GPLv3',
              'Operating System :: OS Independent',
              'Programming Language :: Python',
              'Natural Language :: French',
              'Topic :: Scientific/Engineering :: GIS'],
      packages=['onegeo_manager'],
      install_requires=[
                'aiohttp>=1.2.0',
                'async_timeout>=1.1.0',
                'neogeo_xml_utils==0.0.1',
                'PyPDF2>=1.26.0'],
      dependency_links=[
                'git+https://github.com/neogeo-technologies/neogeo-xml-utils.git#egg=neogeo_xml_utils=0.0.1'])
