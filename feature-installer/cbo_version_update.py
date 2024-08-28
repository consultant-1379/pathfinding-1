import re
import sys
import requests
import base64
import os
job_link = "https://arm.seli.gic.ericsson.se/artifactory/docker-v2-global-local/proj-am/sles/sles-openjdk11/"
job_page = requests.get(job_link)
job_contents = job_page.content
job = job_contents.split("\n")
list2 = []
for line1 in job:
	if '<a href="' in line1 and '    -' in line1 and '_uploads' not in line1 and 'latest' not in line1:
		l2 = []
		l3 = []
		l2 = line1.split('<a href="')
		l3 = l2[1].split('/')
		list2.append(l3[0])
		
list2.sort()
last_element = list2[len(list2)-1].split('.')[0]
list3 = []
for i in range(len(list2)):
	if (list2[i].split('.')[0] == last_element):
		list3.append(list2[i].split('.')[1])
list4 = []
for i in list3:
	j = int(i)
	list4.append(j)
list4.sort()
final_value = str(last_element)+'.'+str(list4[len(list4)-1])
list5=[]
for line1 in list2:
	if final_value in line1:
		list5.append(line1)
print(list5[len(list5)-1])
final_version = list5[len(list5)-1]
os.system('sed -i "s/image-base-os-version:/image-base-os-version: '+final_version+'/g" common-properties.yaml')

