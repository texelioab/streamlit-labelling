import streamlit as st
import hashlib
import pandas as pd
import random
import matplotlib.pyplot as plt


#ES
from src.utils import create_es_client, insert_document, search_document, join_sl_and_los

ELASTIC_HOST = st.secrets["ELASTIC_HOST"]
ELASTIC_USER = st.secrets["ELASTIC_USER"]
ELASTIC_PASS = st.secrets["ELASTIC_PASS"]
client = create_es_client(ELASTIC_HOST, ELASTIC_USER, ELASTIC_PASS)

d_topic_to_id = {t['name']:t['id'] for t in search_document(client, 'topic_entity',{})}
d_id_to_topic = {i[1]:i[0] for i in d_topic_to_id.items()}
d_topic_to_id['None'] = 'none0'
tids = [i[1] for i in d_topic_to_id.items()]
topics = sorted([i[1] for i in d_id_to_topic.items()])
tks = [i[0] for i in d_topic_to_id.items()]
tks.insert(0,tks.pop(tks.index('None')))


def topic_insertion():
    st.session_state.reset = False

    data = {}

    st.title('Topic Insertion for Labelling')

    st.sidebar.markdown("""
    Prepare a new topic for labelling. Kindly input:
    * **Topic Name**
    * **Topic Definition:** Define the topic, what the concept means, represents.
    * **Parent Topic:** Default is None, otherwise select the appropriate parent topic.
    * **Language:** Default is English, otherwise select correct language.
    * **Keywords:** Provide any keywords that typically identify the topic.
    * **Name Variations:** Give any other common formulations for the topic, other ways in which it is commonly referred to.
    * **Difficult Cases:** Describe any cases that could make identification of the topic difficult or confusing.
    * **Helpful Labelled Sentences:** For each input sentence, enter the Sentence Text, the label of whether the topic is present, and an explanation for why this label was given.
    * **Number of Sentences to Generate**: Based on the above inputs, GPT will generate this many new sentences, which you will then review.
    * **Number of sentences in Optimised Suggestion**: GPT will also provide a suggestion for which sentences would together cretate a nuanced set of training sentences for labelling. This will be an n-shot model, where n is the given input.
    """)


    if "reset" not in st.session_state:
        st.session_state.reset = False

    default_values = {"topic_name": '', "topic_definition": '', "parent_topic": 'None', "language": 'English'}

    #Topic Name and Topic Definition
    with st.container():
        topic_name = default_values["topic_name"] if st.session_state.reset else st.session_state.get("topic_name", default_values["topic_name"])
        topic_definition = default_values["topic_definition"] if st.session_state.reset else st.session_state.get("topic_definition", default_values["topic_definition"])

        st.session_state.topic_name = st.text_input('Topic Name', value=topic_name)
        st.session_state.topic_definition = st.text_input('Topic Definition', value=topic_definition)

    if st.session_state.reset:
        st.session_state['parent_topic'] = default_values['parent_topic']
        st.session_state['language'] = default_values['language']
        st.session_state.reset = False

    #Parent Topic and Language
    parent_topic = st.selectbox('Parent Topic', options=tks, index=list(tks).index(st.session_state.get("parent_topic", default_values["parent_topic"])), key='parent_topic')
    parent_topic_id = d_topic_to_id[parent_topic]

    languages = ['English','German','Swedish']
    language = st.selectbox('Language', options=languages, index=languages.index(st.session_state.get("language", default_values["language"])), key='language')

    #Initialise data
    data['topic_name'] = topic_name
    data['topic_definition'] = topic_definition
    data['parent_topic_id'] = parent_topic_id
    data['language'] = language
    data['keywords'] = []
    data['name_variations'] = []
    data['difficult_cases'] = []
    data['labelled_sentences'] = []

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

    for field_set in range(1, st.session_state.num_fields_sentences + 1):
        with st.container():
            st.subheader(f"Labelled Sentence {field_set}")
            sentence_text = st.text_input(r"$\textsf{\large Sentence Text}$", key=f"sentence_field_1_{field_set}")
            label = st.selectbox(r'$\textsf{\large Label}$', ['Yes', 'No'], index=1, key=f"sentence_field_2_{field_set}")
            explanation = st.text_input(r"$\textsf{\large Explanation}$", key=f"sentence_field_3_{field_set}")
            data['labelled_sentences'].append({'sentence_text':sentence_text,'label':label,'explanation':explanation})
    
    st.button("Add Labelled Sentence", on_click=add_sentence,type='primary')    
    if st.session_state.num_fields_sentences > 0:
        st.button("Delete Sentence", on_click=remove_sentence)
    
    data['n_new_sentences'] = st.selectbox('Number of sentences to generate', [5,10,20], index=0,key='n_new_sentences')
    data['n_gpt_suggestions'] = st.selectbox('Number of sentences in optimised suggestion', list(range(1,11)),index=4,key='n_gpt_suggestions')

    #Save Data
    def save_data():
        with st.spinner('Your input is being processed. This should only take a few moments.'):
            from src.user_input_maximisation import input_maximised
            new = input_maximised(3, data['n_new_sentences'], data['n_gpt_suggestions'], data['topic_name'], data['topic_definition'], data['keywords'], data['name_variations'], data['difficult_cases'], data['labelled_sentences'])
            for k, v in new.items():
                data[k] = v
            st.session_state.data_variable = data
        st.write('Press "Confirm Save" Again')
        st.session_state.page = 'customisation'

    if 'show_save_confirmation' not in st.session_state:
        st.session_state.show_save_confirmation = False

    def confirm_save():
        st.session_state.show_save_confirmation = True

    def cancel_save():
        st.session_state.show_save_confirmation = False

    st.write('#####')
    st.button('Save', on_click=confirm_save)

    if st.session_state.show_save_confirmation:
        with st.container():
            st.warning('Are you sure you want to save?')
            col1, col2 = st.columns(2)
            with col1:
                if st.button('Confirm Save'):
                    save_data()
            with col2:
                if st.button('Cancel', on_click=cancel_save):
                    pass


def existing_sentence_database():
    #Access sentences with labels
    l_from_join = join_sl_and_los(client)
    ls = []
    for dictionary in l_from_join:
        ls.append({'sentence_text':dictionary['sentence_text'],'topics':sorted([d_id_to_topic[tid] for tid in dictionary['topic_id']])})
    data = []
    for d in ls:
        l = [d['sentence_text']]
        l.extend([0 for i in range(len(topics))])
        txt = ''
        for t in d['topics']:
            l[1+topics.index(t)] += 1
            txt += t + ', '
        l.append(txt[:-2])
        data.append(l)
    
    #Create Dynamic DataFrame
    random.seed(13)
    random.shuffle(data)
    columns = ['Sentence']
    columns.extend(topics)
    columns.append('Topics')
    df = pd.DataFrame(data,columns=columns)
    st.title('Existing Topics and Sentences')
    selected_topics = st.sidebar.multiselect('Select Topics', topics)

    def filter_data(selected_topics, df, topics):
        if not selected_topics:
            return df
        else:
            mask = df[topics].apply(lambda row: any(row[topic] == 1 for topic in selected_topics), axis=1)
            return df[mask]

    filtered_data = filter_data(selected_topics, df, topics)

    st.header('Display Sentences and Topics')
    st.write(str(filtered_data.shape[0]) + ' rows')
    st.dataframe(filtered_data[['Sentence', 'Topics']].reset_index(drop=True))

    #Plot Labelled Sentences Per Topic
    df_plot = df[topics]
    column_sums = df_plot.sum(axis=0)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.bar(topics, column_sums,width=0.5,color='0.8', edgecolor='black')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set(ylabel='Sentence Count',title='Number of Labelled Sentences Per Topic')
    plt.xticks(rotation=90,fontsize=6)
    plt.subplots_adjust(bottom=0.45)
    plt.tight_layout()
    st.pyplot(fig)


def example_topic_entry():
    st.title('Example Topic Entry')
    st.header('An example of how one could insert a new topic')
    st.markdown("""* **Topic Name:** Fluctuating Demand Change
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
    * Name Variation 1: Regular Demand Change
    * Name Variation 2: Demand Change
* **Difficult Cases:**
    * Difficult Case 1: Just because prices change, this does not necessarily mean that demand has changed.
    * Difficult Case 2: Note the distinction between fluctuating demand change and shock and seasonal demand change: changes in demand due to economic shocks and seasonal influences, respectively.
* **Labelled Sentence 1:**
    * Sentence Text: Further contributing to demand-pull inflation has been the marked change in the composition of household demand.
    * Label: Yes
    * Explanation: Inflation relates to prices changing, which does not in itself mean demand has changed; however, the sentence mentions that the composition of household demand has seen a marked change, and since there is no mention of any seasonal or shock influence, this is Fluctuating demand change.
* **Labelled Sentence 2:**
    * Sentence Text: Apart from basic food items, electricity (200 Kilowatts) and (50 Kilowatts) also recorded a price decrease by 2.1 per cent and 2.5 per cent respectively in comparison to rates recorded in August.
    * Label: No
    * Explanation: This sentence mentions nothing about demand, and although it is possible that the discussed price changes may be caused by a change in demand, we cannot be sure, and so we do not label with a change in demand.
* **Labelled Sentence 3:**
    * Sentence Text: Cirrus Australia East has seen a marked increase in demand for aircraft in the post-pandemic era, driven by customers' inability to rely on airlines.
    * Label: No
    * Explanation: Although clearly demand is seen to have changed, this is shock demand change, not Fluctuating demand change, as the sentence mentions that demand has changed in the post-pandemic era, relating to the Covid-19 pandemic, which was an economic shock; however, since this being a shock is not explicit, this is a difficult case.
* **Labelled Sentence 4:**
    * Sentence Text: Teekay Tankers added that “looking ahead, an increase in oil demand over the medium-term is expected, with the IEA forecasting 1.7 million barrels per day (mb/d) growth in 2022 and a further 2.1 mb/d growth in 2023, despite high oil prices and concerns over the health of the global economy.”
    * Label: Yes
    * Explanation: The sentence mentions that demand for oil will change over the medium-term, and since there is no mention of any seasonal or shock influence on demand, this is Fluctuating demand change.
* **Labelled Sentence 5:**
    * Sentence Text: But it could be more of a blip than a trend, and demand may retreat once the holiday is further in the rearview mirror.
    * Label: No
    * Explanation: Although clearly there is demand change in this sentence, this demand change is seasonal, not Fluctuating, since it is mentioned that the change in demand may revert once the holiday is over, where the holiday denotes a seasonal rather than Fluctuating effect; since the holiday effect is not explicitly seasonal, this is a somewhat difficult case.
""")


#Password Verification
def verify_password(input_password, stored_hashed_password):
    return hashlib.sha256(input_password.encode()).hexdigest() == stored_hashed_password

def check_password():
    if verify_password(st.session_state['password'], st.secrets["PASSWORD_HASH"]):
        st.session_state['authenticated'] = True
    else:
        st.session_state['authenticated'] = False
        st.error('Incorrect password')

if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

if not st.session_state['authenticated']:
    st.text_input("Password", type='password', on_change=check_password, key='password')
else:
    if 'page' not in st.session_state:
        st.session_state.page = 'main_page'
    
    if st.session_state.page == 'main_page':
        selection = 'Existing Sentence Database'

        st.sidebar.title('Navigation')
        options = ['Existing Sentence Database', 'Example Topic Entry', 'Topic Insertion']
        selection = st.sidebar.radio("Go to", options)
        if selection == 'Topic Insertion':
            topic_insertion()
        elif selection == 'Existing Sentence Database':
            existing_sentence_database()
        elif selection == 'Example Topic Entry':
            example_topic_entry()
    
    elif st.session_state.page == 'customisation':
        selection = 'Configuration Start'
        data = st.session_state.data_variable

        if "selected_sentences" not in st.session_state:
            st.session_state.selected_sentences = []

        st.sidebar.title('Navigation')
        options = ['Configuration Start', 'Keywords', 'Name Variations', 'Difficult Cases', 'Browse and Select Sentences', 'Sentence Suggestions', 'Confirmation']
        selection = st.sidebar.radio("Go to", options)

        if 'selected_keywords' not in st.session_state:
            st.session_state.selected_keywords = []
        if 'selected_name_variations' not in st.session_state:
            st.session_state.selected_name_variations = []
        if 'selected_difficult_cases' not in st.session_state:
            st.session_state.selected_difficult_cases = []
        if selection != "Keywords" and 'current_keywords' in st.session_state:
            st.session_state.selected_keywords = st.session_state.current_keywords
        if selection != "Name Variations" and 'current_name_variations' in st.session_state:
            st.session_state.selected_name_variations = st.session_state.current_name_variations
        if selection != "Difficult Cases" and 'current_difficult_cases' in st.session_state:
            st.session_state.selected_difficult_cases = st.session_state.current_difficult_cases

        prompt_choice_options = ['GPT Suggestion', 'Cluster Suggestion', 'Selected Sentences']
        if 'prompt_choice' not in st.session_state:
            st.session_state.prompt_choice = 'Selected Sentences'
        if selection != 'Sentence Suggestions' and 'current_prompt_choice' in st.session_state:
            st.session_state.prompt_choice = st.session_state.current_prompt_choice
        
        for i, sentence in enumerate(data['labelled_sentences'] + data['gpt_sentences'],start=1):
            key = f'sentence_{i}'
            if key not in st.session_state:
                st.session_state[key] = False


        if selection == 'Configuration Start':
            st.title('Topic Optimisation')
            st.write('''You will now optimise the prompt for labelling, using feedback generated from your initial input.
                     Using the navigation in the sidebar, select the keywords, name variations, difficult cases and sentences that are the most relevant.''')
        elif selection == 'Keywords':
            st.title('Keywords')
            l = [w.capitalize() for w in data['keywords']+data['new_keywords']]
            st.session_state.current_keywords = st.multiselect('Select Keywords', sorted(list(set(l))),key='keywords_multiselect',default=st.session_state.selected_keywords)
            st.subheader('Selected Keywords')
            for w in st.session_state.current_keywords:
                st.write(w)
        elif selection == 'Name Variations':
            st.title('Name Variations')
            l = [w.capitalize() for w in data['name_variations']+data['new_name_variations']]
            st.session_state.current_name_variations = st.multiselect('Select Name Variations', sorted(list(set(l))),key='name_variations_multiselect',default=st.session_state.selected_name_variations)
            st.subheader('Selected Name Variations')
            for w in st.session_state.current_name_variations:
                st.write(w)
        elif selection == 'Difficult Cases':
            st.title('Difficult Cases')
            l = data['difficult_cases']+data['new_difficult_cases']
            st.session_state.current_difficult_cases = st.multiselect('Select Difficult Cases', sorted(list(set(l))),key='difficult_cases_multiselect',default=st.session_state.selected_difficult_cases)
            st.subheader('Selected Difficult Cases')
            for w in st.session_state.current_difficult_cases:
                st.write(w)
        elif selection == 'Browse and Select Sentences':
            st.title('Browsing and Conditional Sentence Selection')
            st.write('Below are the input sentences and the GPT-generated sentences. In no particular order. On the next page are two suggestions for groups of sentences for the prompt. Look through the sentences below, familiarise, and then consider these suggestions. If neither one is satisfactory, return here and select the sentences you desire. Note that if sentences are selected here while one of the suggestions is also selected, the suggestion takes precedence.')
            for i, sentence in enumerate(data['labelled_sentences'] + data['gpt_sentences'],start=1):
                st.write(f'Sentence {i}')
                key = f'sentence_{i}'
                message = f"**Sentence Text:**  \n{sentence['sentence_text']}  \n**Label:** {sentence['label']}  \n**Explanation:**  \n{sentence['explanation']}"
                if st.session_state[key]:
                    st.success(message)
                    if sentence not in st.session_state.selected_sentences:
                        st.session_state.selected_sentences.append(sentence)                        
                else:
                    st.error(message)
                    if sentence in st.session_state.selected_sentences:
                        st.session_state.selected_sentences.remove(sentence)

                col1, col2 = st.columns(2)
                def select_sentence(key):
                    st.session_state[key] = True
                def unselect_sentence(key):
                    st.session_state[key] = False
                with col1:
                    st.button('Select',on_click=select_sentence, args=(key,),key=f'succes_{key}')
                with col2:
                    st.button('Unselect',on_click=unselect_sentence, args=(key,),key=f'error_{key}')

        elif selection == 'Sentence Suggestions':
            st.session_state.current_prompt_choice = st.sidebar.selectbox("Choice",prompt_choice_options,index=prompt_choice_options.index(st.session_state.prompt_choice), key='prompted_choice')

            st.header('GPT Suggestion')
            st.subheader(f'Derived by asking GPT to select the {data['n_gpt_suggestions']} sentences which together would provide the largest amount of context and data variation.')
            for i, sentence in enumerate(data['gpt_suggestion'],start=1):
                st.write(rf"$\textsf{{\large Sentence {i}}}$")
                st.write( f"**Sentence Text:**  \n{sentence['sentence_text']}  \n**Label:** {sentence['label']}  \n**Explanation:**  \n{sentence['explanation']}")
            
            st.header('Clustering Suggestion')
            st.subheader('Derived by clustering all sentences and selecting one sentence from each cluster. Yes and No automatically creates two main clusters, so within each cluster we cluster again by sentence text and explanation.')
            for i, sentence in enumerate(data['cluster_suggestion'],start=1):
                st.write(rf"$\textsf{{\large Sentence {i}}}$")
                st.write( f"**Sentence Text:**  \n{sentence['sentence_text']}  \n**Label:** {sentence['label']}  \n**Explanation:**  \n{sentence['explanation']}")
        elif selection == 'Confirmation':
            st.title('Selection Confirmation')
            st.header('Review the selections made on the previous pages. Accept them, or if dissatisfied, return to the relevant page and make any additionaly adjustments required.')
            st.subheader('Keywords')
            for w in st.session_state.selected_keywords:
                st.write(w)
            st.subheader('Name Variations')
            for w in st.session_state.selected_name_variations:
                st.write(w)
            st.subheader('Difficult Cases')
            for w in st.session_state.selected_difficult_cases:
                st.write(w)
            st.subheader(f'Sentences  \nChoice: {st.session_state.prompt_choice}')
            if st.session_state.prompt_choice == 'GPT Suggestion':
                sentences = data['gpt_suggestion']
            elif st.session_state.prompt_choice == 'Cluster Suggestion':
                sentences = data['cluster_suggestion']
            else:
                sentences = st.session_state.selected_sentences
            for i, sentence in enumerate(sentences,start=1):
                st.write(f"Sentence {i}\nSentence Text: {sentence['sentence_text']}\nLabel: {sentence['label']}\nExplanation: {sentence['explanation']}")


            #Save Data
            def reset_state(keep_authenticated=True):
                keys_to_preserve = {'authenticated'} if keep_authenticated else set()
                
                for key in list(st.session_state.keys()):
                    if key not in keys_to_preserve:
                        del st.session_state[key]
            
            def save_data():

                labeller_id = '0kgu5o0Bzhy8p2ulxOM5'

                #create topic id for new topic
                i = 40
                while True:
                    if f'c{i}' not in tids:
                        id = f'c{i}'
                        break
                    i += 1

                t = 'topic' if data['parent_topic_id'] == 'none0' else 'subtopic'
                
                insert_document(client, 'topic_entity',{'id':id,'name':data['topic_name'], 'type':t,'parent_topic_id':data['parent_topic'],'labeller_id':labeller_id})
                insert_document(client, 'topic_entity_definition',{'topic_id':id,'name':data['topic_name'],'definition':data['topic_definition'],'language':data['language'],'status':'Draft','keyword':st.session_state.selected_keywords,'name_variation':st.session_state.selected_name_variations,'difficult_case':st.session_state.selected_difficult_cases})
                for sentence in sentences:
                    insert_document(client, 'labelled_sentence',{'sentence_text':sentence['sentence_text'],'translated':False,'generated':False,'parent_sentence_id':'none0'})
                client.indices.refresh(index='labelled_sentence')
                label_confidence_dict = {"Yes": 1, "No": 0}
                for sentence in data['prompt_choice']:
                    sentence_id = search_document(client, 'labelled_sentence',{'sentence_text':sentence['sentence_text']},all=True)[0]['_id']
                    insert_document(client, 'sentence_label', {'labeller_id':labeller_id,'sentence_id':sentence_id,'topic_id':id,'position_in_text':-1,'confidence':label_confidence_dict[sentence['label']],'explanation':sentence['explanation']})
                
                reset_state()
                st.session_state.reset = True
                st.rerun()


            if 'show_save_confirmation2' not in st.session_state:
                st.session_state.show_save_confirmation2 = False

            def confirm_save():
                st.session_state.show_save_confirmation2 = True

            def cancel_save():
                st.session_state.show_save_confirmation2 = False

            st.write('#####')
            st.button('Save', on_click=confirm_save)

            if st.session_state.show_save_confirmation2:
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