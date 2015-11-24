#put ipython.tar.gz into /home/duxbury/
cd /home/duxbury/
tar xzvf ipython.tar.gz
# Create src directory
mkdir src
cd src
# put ipython-dep into src folder
mv ipython-dep.tar.gz /home/duxbury/src/

#need to check if you have python2.7 installed - test command
if ! type python2.7; then
	tar xf Python-2.7.5.tar.xz
	cd Python-2.7.5
	./configure --prefix=/usr/bin
	make && make altinstall
	cd ..
fi

#wget --no-check-certificate https://bootstrap.pypa.io/get-pip.py

python2.7 get-pip.py

# can be installed from tar file 
# wget --no-check-certificate https://pypi.python.org/packages/source/i/ipython/ipython-3.2.0.tar.gz#md5=41aa9b34f39484861e77c46ffb29b699
tar xzvf ipython-3.2.0.tar.gz

cd ipython-3.2.0
sudo python2.7 setup.py install
cd ..
# can be installed from tar file (or yum)
#wget https://downloads.sourceforge.net/project/matplotlib/matplotlib/matplotlib-1.4.3/matplotlib-1.4.3.tar.gz
tar xzvf matplotlib-1.4.3.tar.gz

cd matplotlib-1.4.3
sudo python2.7 setup.py build
sudo python2.7 setup.py install
cd ..

pip2.7 install pyzmq
pip2.7 install jinja2
pip2.7 install tornado
pip2.7 install jsonschema
pip2.7 install scipy
pip2.7 install statsmodels
cd ~

tar xzvf ipython.tar.gz
cd ipython
./runIpython.sh

