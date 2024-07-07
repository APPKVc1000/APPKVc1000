#!/usr/bin/env python
# coding: utf-8

# In[1]:


import io
import pandas
import json
from collections import defaultdict
# from collections import OrderedDict, defaultdict


# In[2]:


with open(r"C:\Users\APPKVc1000\projectDuck\duck.json", 'r', encoding='utf-8') as duck:
    domains = tuple((json.loads(domain) for domain in duck.readlines()))
    duck.close()


# In[3]:


kingdom = defaultdict(list)

for domain in domains:
    for website, data in domain.items():
        if isinstance(data, str):
            kingdom[website].append(data)

kingdom = {website: tuple(data) for website, data in kingdom.items()}


# In[4]:


phylum = defaultdict(list)

for domain in domains:
    for webpage, data in domain.items():
        if isinstance(data, dict):
            phylum[webpage].append(data)

phylum = {webpage: tuple(data) for webpage, data in phylum.items()}


# In[5]:


classes = list()

for webpage, data in phylum.items():
    for duck in data:
        classes.append(duck)

classes = tuple(classes)

families = list()
genera = list()
species = list()

for data in classes:
    for classes_data, order_data in data.items():
        for order in order_data:
            if isinstance(order, dict):
                for family, genus in order.items():
                    families.append(family)
                    genera.append(genus)
            elif isinstance(order, str):
                species.append(order)
            else:
                raise ValueError

families = tuple(families)
genera = tuple(genera)
species = tuple(species)


# In[6]:


len(domains)


# In[7]:


display(len(kingdom))
kingdom_population = 0
for website, data in kingdom.items(): 
    if isinstance(data, str): 
        kingdom_population += 1
    else: 
        kingdom_population += len(data)
kingdom_population


# In[8]:


display(len(phylum))
phylum_population = 0
for webpage, data in phylum.items(): 
    if isinstance(data, str): 
        phylum_population += 1
    else: 
        phylum_population += len(data)
phylum_population


# In[9]:


len(domains) == phylum_population + kingdom_population


# In[10]:


domains


# In[11]:


kingdom


# In[12]:


phylum


# In[13]:


class_population = 0
for data in classes:
    for classes_data, order_data in data.items():
        if isinstance(classes_data, str):
            class_population += 1
        else:
            raise ValueError
class_population


# In[14]:


order_population = 0
for data in classes:
    for classes_data, order_data in data.items():
        for order in order_data:
            if isinstance(order, dict):
                for family, genus in order.items():
                    order_population += 1
            elif isinstance(order, str):
                order_population += 1
            else:
                raise ValueError
order_population


# In[15]:


len(families)


# In[16]:


len(genera)


# In[17]:


len(species)


# In[18]:


families


# In[19]:


genera


# In[20]:


species


# In[21]:


len(tuple(sorted(list(filter(None, set(families))), reverse=True)))


# In[22]:


len(tuple(sorted(list(filter(None, set(genera))), reverse=True)))


# In[23]:


len(tuple(sorted(list(filter(None, set(species))), reverse=True)))


# In[24]:


tuple(sorted(list(filter(None, set(families))), reverse=True))


# In[25]:


tuple(sorted(list(filter(None, set(genera))), reverse=True))


# In[26]:


tuple(sorted(list(filter(None, set(species))), reverse=True))


# In[27]:


from translate import Translator
import pprint

# domain = response.xpath("/*[@lang]/@lang").get()

data = dict()

for webpage in phylum["('Hyakujuu Sentai Gaoranger', 'https://tl.wikipedia.org/wiki/Hyakujuu_Sentai_Gaoranger')"]:
    pprint.pp(webpage)
    print()

    for classes, order in webpage.items():
        classes = Translator(provider='mymemory', from_lang='tl', to_lang='en').translate(classes)
        
        for classes_data, order_data in enumerate(order):
            if isinstance(order_data, str):
                order[classes_data] = Translator(provider='mymemory', from_lang='tl', to_lang='en').translate(order_data)
            elif isinstance(order_data, dict):
                order[classes_data] = dict(zip(
                    [Translator(provider='mymemory', from_lang='tl', to_lang='en').translate(family) for family in list(order_data.keys())],
                    [genus for genus in order_data.values()]
                ))
            else:
                raise ValueError

        data.update({classes: order})
    
pprint.pp(data)


# In[28]:


exit()


# In[ ]:




