 #!/bin/bash
 
 #   lda  stage
source /etc/duxbay.conf

#  copy solution files to all nodes 
# use array scp 
for d in "${NODES[@]}" 
do 
	scp -r ${LUSER}/ml $d:${LUSER}/.    
done

