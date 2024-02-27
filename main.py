import streamlit as st
import hashlib
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


def verify_password(input_password, stored_hashed_password):
    return hashlib.sha256(input_password.encode()).hexdigest() == stored_hashed_password

def check_password():
    if verify_password(st.session_state['password'], st.secrets["PASSWORD_HASH"]):
        st.session_state['authenticated'] = True
    else:
        st.session_state['authenticated'] = False
        st.error('Incorrect password')

#ES
def create_es_client() -> Elasticsearch:
    elastic_client = Elasticsearch(
    cloud_id=st.secrets["ELASTIC_HOST"],
    basic_auth=(st.secrets["ELASTIC_USER"],st.secrets["ELASTIC_PASS"])
)
    return elastic_client

def insert_document(index: str, document: dict) -> None:
    client.index(index=index, document=document)

def match_query(identifier: dict) -> dict:
    return {
        'query': {
            'bool': {
                'must': [
                    {'match': {key: value}} for key, value in identifier.items()
                ]
            }
        }
    }

def search_document(index: str, identifier: dict, all=False, batch_size=1000) -> list:
    documents = []
    query = {"query": {"match_all": {}}} if not identifier else {"query": match_query(identifier)['query']}
    for hit in scan(client, index=index, query=query, size=batch_size):
        if not all:
            documents.append(hit['_source'])
        else:
            documents.append(hit)
    return documents

client = create_es_client()
d_topic_to_parent_id = {t['name']: t['id'] for t in search_document('topic_entity',{})}
d_topic_to_parent_id['None'] = 'none0'
tids = [i[1] for i in d_topic_to_parent_id.items()]


if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

if not st.session_state['authenticated']:
    st.text_input("Password", type='password', on_change=check_password, key='password')
else:

    st.markdown('[''](#section-1)')
    st.session_state.reset = False

        
    data = {}

    st.title('Topic Insertion for Labelling')

    st.markdown("""
    Prepare a new topic for labelling. Kindly input:
    * **Topic Name**
    * **Topic Definition:** Define the topic, what the concept means, represents.
    * **Parent Topic:** Default is None, otherwise select the appropriate parent topic.
    * **Language:** Default is English, otherwise select correct language.
    * **Keywords:** Provide any keywords that typically identify the topic.
    * **Name Variations:** Give any other common formulations for the topic, other ways in which it is commonly referred to.
    * **Difficult Cases:** Describe any cases that could make identification of the topic difficult or confusing.
    * **Helpful Labelled Sentences:** For each input sentence, enter the Sentence Text, the confidence that the topic is present, and an explanation for why this confidence score was given.
    """)


    if "reset" not in st.session_state:
        st.session_state.reset = False

    default_values = {"topic_name": '', "topic_definition": '', "parent_topic": 'None', "language": 'English'}

    with st.container():

        topic_name = default_values["topic_name"] if st.session_state.reset else st.session_state.get("topic_name", default_values["topic_name"])
        topic_definition = default_values["topic_definition"] if st.session_state.reset else st.session_state.get("topic_definition", default_values["topic_definition"])

        st.session_state.topic_name = st.text_input('Topic Name', value=topic_name)
        st.session_state.topic_definition = st.text_input('Topic Definition', value=topic_definition)


    if st.session_state.reset:
        st.session_state['parent_topic'] = default_values['parent_topic']
        st.session_state['language'] = default_values['language']
        st.session_state.reset = False

    parent_topic = st.selectbox('Parent Topic', options=d_topic_to_parent_id.keys(), index=list(d_topic_to_parent_id.keys()).index(st.session_state.get("parent_topic", default_values["parent_topic"])), key='parent_topic')
    parent_topic_id = d_topic_to_parent_id[parent_topic]

    languages = ['English','German','Swedish']
    language = st.selectbox('Language', options=languages, index=languages.index(st.session_state.get("language", default_values["language"])), key='language')


    data['topic_name'] = topic_name
    data['topic_definition'] = topic_definition
    data['parent_topic_id'] = parent_topic_id
    data['language'] = language
    data['keywords'] = []
    data['name_variations'] = []
    data['difficult_cases'] = []
    data['sentences'] = []
    
    #keyword
    st.write('#####')
    if 'num_fields_keywords' not in st.session_state:
        st.session_state.num_fields_keywords = 0
    def add_keyword():
        st.session_state.num_fields_keywords += 1
    def remove_keyword():
        st.session_state.num_fields_keywords -= 1


    for field_set in range(1, st.session_state.num_fields_keywords + 1):
        with st.container():
            keyword = st.text_input(f'Keyword {field_set}', key=f"keyword_field_1_{field_set}")
            data['keywords'].append(keyword)
    
    st.button("Add Keyword", on_click=add_keyword,type='primary')
    if st.session_state.num_fields_keywords > 0:
        st.button("Delete Keyword", on_click=remove_keyword)

    #name variations
    st.write('#####')
    if 'num_fields_name_variations' not in st.session_state:
        st.session_state.num_fields_name_variations = 0
    def add_name_variation():
        st.session_state.num_fields_name_variations += 1
    def remove_name_variation():
        st.session_state.num_fields_name_variations -= 1

    for field_set in range(1, st.session_state.num_fields_name_variations + 1):
        with st.container():
            name_variation = st.text_input(f'Name Variation {field_set}', key=f"name_variation_field_1_{field_set}")
            data['name_variations'].append(name_variation)

    st.button("Add Name Variation", on_click=add_name_variation,type='primary')    
    if st.session_state.num_fields_name_variations > 0:
        st.button("Delete Name Variation", on_click=remove_name_variation)

    #difficult cases
    st.write('#####')
    if 'num_fields_difficult_cases' not in st.session_state:
        st.session_state.num_fields_difficult_cases = 0
    def add_difficult_case():
        st.session_state.num_fields_difficult_cases += 1
    def remove_difficult_case():
        st.session_state.num_fields_difficult_cases -= 1

    for field_set in range(1, st.session_state.num_fields_difficult_cases + 1):
        with st.container():
            difficult_case = st.text_input(f'Difficult Case {field_set}', key=f"difficult_case_field_1_{field_set}")
            data['difficult_cases'].append(difficult_case)

    st.button("Add Difficult Case", on_click=add_difficult_case,type='primary')    
    if st.session_state.num_fields_difficult_cases > 0:
        st.button("Delete Difficult Case", on_click=remove_difficult_case)

    #sentence
    st.write('#####')
    if 'num_fields_sentences' not in st.session_state:
        st.session_state.num_fields_sentences = 0
    def add_sentence():
        st.session_state.num_fields_sentences += 1
    def remove_sentence():
        st.session_state.num_fields_sentences -= 1


    def is_number(input_string):
        if input_string == '':
            return True
        try:
            float(input_string)
            return True
        except ValueError:
            return False

    for field_set in range(1, st.session_state.num_fields_sentences + 1):
        with st.container():
            st.subheader(f"Labelled Sentence {field_set}")
            sentence_text = st.text_input(r"$\textsf{\large Sentence Text}$", key=f"sentence_field_1_{field_set}")
            confidence = st.text_input(r"$\textsf{\large Confidence}$", key=f"sentence_field_2_{field_set}")
            if not is_number(confidence):
                st.error('Please enter a number for confidence.')
            else:
                confidence = float(confidence) if confidence else None
            explanation = st.text_input(r"$\textsf{\large Explanation}$", key=f"sentence_field_3_{field_set}")
            data['sentences'].append({'sentence_text':sentence_text,'confidence':confidence,'explanation':explanation})

    st.button("Add Labelled Sentence", on_click=add_sentence,type='primary')    
    if st.session_state.num_fields_sentences > 0:
        st.button("Delete Sentence", on_click=remove_sentence)

    st.write('#####')
    def reset_state(keep_authenticated=True):
        keys_to_preserve = {'authenticated'} if keep_authenticated else set()
        
        for key in list(st.session_state.keys()):
            if key not in keys_to_preserve:
                del st.session_state[key]


    def save_data():
        data['keywords'] = str(data['keywords'])
        data['name_variations'] = str(data['name_variations'])
        data['difficult_cases'] = str(data['difficult_cases'])
        labeller_id = '0kgu5o0Bzhy8p2ulxOM5'
        i = 40
        while True:
            if f'c{i}' not in tids:
                id = f'c{i}'
                break
            i += 1
        t = 'topic' if parent_topic_id == 'none0' else 'subtopic'
        
        insert_document('topic_entity',{'id':id,'name':topic_name, 'type':t,'parent_topic_id':parent_topic,'labeller_id':labeller_id})
        insert_document('topic_entity_definition',{'topic_id':id,'name':topic_name,'definition':topic_definition,'language':language,'status':'Draft','keyword':data['keywords'],'name_variation':data['name_variations'],'difficult_case':data['difficult_cases']})
        for sentence in data['sentences']:
            insert_document('labelled_sentence',{'sentence_text':sentence['sentence_text'],'translated':False,'generated':False,'parent_sentence_id':'none0'})
        client.indices.refresh(index='labelled_sentence')
        for sentence in data['sentences']:
            sentence_id = search_document('labelled_sentence',{'sentence_text':sentence['sentence_text']},all=True)[0]['_id']
            insert_document('sentence_label', {'labeller_id':labeller_id,'sentence_id':sentence_id,'topic_id':id,'position_in_text':-1,'confidence':float(sentence['confidence']),'explanation':sentence['explanation']})

        reset_state()
        st.session_state.reset = True
        st.rerun()

        
    if 'show_save_confirmation' not in st.session_state:
        st.session_state.show_save_confirmation = False

    def confirm_save():
        st.session_state.show_save_confirmation = True

    def cancel_save():
        st.session_state.show_save_confirmation = False

    st.button('Save', on_click=confirm_save)

    if st.session_state.show_save_confirmation:
        with st.container():
            st.warning('Are you sure you want to save?')
            col1, col2 = st.columns(2)
            with col1:
                if st.button('Confirm Save'):
                    st.session_state.reset = True
                    save_data()
            with col2:
                if st.button('Cancel', on_click=cancel_save):
                    pass


    st.markdown("""## Sample New Topic Entry
* **Topic Name:** Fluctuating Demand Change
* **Topic Definition:** Fluctuating demand change is when demand for a product changes due to natural variation in the economic environment. If the change in demand does not occur due to seasonal variation, or because of some economic shock, then the change in demand is Fluctuating. 
* **Parent Topic:** Demand Change
* **Language:** English
* **Keywords:**
    * Keyword 1: Demand
    * Keyword 2: Needs
    * Keyword 3: Interest
    * Keyword 4: Desire
    * Keyword 5: Appetite
* **Name Variations:**
    * Name Variation 1: Regular demand change
    * Name Variation 2: Demand Change
* **Difficult Cases:**
    * Difficult Case 1: Note that just because prices change, this does not necessarily mean that demand has changed.
    * Difficult Case 2: Note the distinction between fluctuating demand change and shock and seasonal demand change.
* **Labelled Sentence 1:**
    * Sentence Text: Further contributing to demand-pull inflation has been the marked change in the composition of household demand
    * Confidence: 95
    * Explanation: Inflation relates to prices changing, which does not in itself mean demand has changed; however, the sentence mentions that the composition of household demand has seen a marked change, and since there is no mention of any seasonal or shock influence, this is Fluctuating demand change.
* **Labelled Sentence 2:**
    * Sentence Text: Teekay Tankers added that “looking ahead, an increase in oil demand over the medium-term is expected, with the IEA forecasting 1.7 million barrels per day (mb/d) growth in 2022 and a further 2.1 mb/d growth in 2023, despite high oil prices and concerns over the health of the global economy.”
    * Confidence: 90
    * Explanation: The sentence mentions that demand for oil will change over the medium-term, and since there is no mention of any seasonal or shock influence on demand, this is Fluctuating demand change.
* **Labelled Sentence 3:**
    * Sentence Text: Cirrus Australia East has seen a marked increase in demand for aircraft in the post-pandemic era, driven by customers' inability to rely on airlines.
    * Confidence: 40
    * Explanation: Although clearly demand is seen to have changed, this is shock demand change, not Fluctuating demand change, as the sentence mentions that demand has changed in the post-pandemic era, relating to the Covid-19 pandemic, which was an economic shock; however, since this being a shock is not explicit, the confidence is still relatively high (although still dismissive)       
* **Labelled Sentence 4:**
    * Sentence Text: But it could be more of a blip than a trend, and demand may retreat once the holiday is further in the rearview mirror.
    * Confidence: 25
    * Explanation: Although clearly there is demand change in this sentence, this demand change is seasonal, not Fluctuating, since it is mentioned that the change in demand may revert once the holiday is over, where the holiday denotes a seasonal rather than Fluctuating effect; since the holiday effect is not explicitly seasonal, confidence is slightly higher.
* **Labelled Sentence 5:**
    * Sentence Text: Apart from basic food items, electricity (200 Kilowatts) and (50 Kilowatts) also recorded a price decrease by 2.1 per cent and 2.5 per cent respectively in comparison to rates recorded in August.
    * Confidence: 10
    * Explanation: This sentence mentions nothing about demand, and although it is possible that the discussed price changes may be caused by a change in demand, we cannot be sure, and so we do not label with a change in demand.
""")
