from src.gpt_augmentation import generate_kw_nv_dc, generate_n_sentences
from src.gpt_sentence_suggestions import select_n_sentences
from cluster_sentences import yes_no_cluster_sentences
import streamlit as st
import random

def input_maximised(n_kw_nv_dc, n_new_sentences, n_gpt_suggestions, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences):
    gpt_key = st.secrets["GPT_TOPICS_KEY"]
    new_keywords, new_name_variations, new_difficult_cases = [], [], []
    for i in range(n_kw_nv_dc):
        new = generate_kw_nv_dc(gpt_key, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences)
        new_keywords.extend(new['keywords'])
        new_name_variations.extend(new['name_variations'])
        new_difficult_cases.extend(new['difficult_cases'])
    
    new_sentences = generate_n_sentences(gpt_key, n_new_sentences, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences)
    sentences = labelled_sentences + new_sentences

    random.shuffle(sentences)
    clusters = yes_no_cluster_sentences(sentences)
    cluster_suggestion = []
    for cluster in clusters:
        for s in cluster:
            b = False
            if topic_name not in s['sentence_text']:
                cluster_suggestion.append(s)
                b = True
                break
        if b:
            continue
        else:
            cluster_suggestion.append(s)

    gpt_suggestion = select_n_sentences(gpt_key, n_gpt_suggestions, sentences, topic_name, topic_definition, keywords, name_variations, difficult_cases)
    return {'labelled_sentences': labelled_sentences, 'gpt_sentences': new_sentences, 'gpt_suggestion':gpt_suggestion, 'cluster_suggestion':cluster_suggestion, 'new_keywords':new_keywords, 'new_name_variations':new_name_variations, 'new_difficult_cases':new_difficult_cases}

if __name__ == "__main__":
    topic_name = 'Corporate Bonds'
    topic_definition = "Corporate bonds are fixed-income investment securities representing ownership of debt, where an investor loans money to a company for a set period of time and receives regular interest payments, providing a means for diversifying portfolios and mitigating investment risk for the investor, and a safe way for the bond issuer, who returns the investor's money once the bond reaches maturity, to access capital."
    keywords = ['Bonds', 'Private Debt', 'Issue', 'Financing', 'Borrow', 'Coupon', 'Maturity', 'Interest']
    name_variations = ['Corporate Debt Security', 'Private Bond', 'Firm Bond']
    difficult_cases = ['Corporate Bonds and Government Bonds are not the same. Distinguishing these can be challenging.', 'Instead of using the word "Bonds" explicitly, these may be referred to as "Debt Instruments" or similar, which might not actually be "Bonds".']

    labelled_sentences = [
        {
            'sentence_text':'Alphabet Takes Advantage of Cheap Borrowing With $10 Billion Bond Sale.',
            'label':'Yes',
            'explanation':'Clearly the sentence discusses bonds. The trick is identifying that Alphabet is a company, which makes the bonds it sells corporate bonds.'
        },
        {
            'sentence_text':'The lower rates for the new assets have led analysts to forecast that demand will be far below that displayed for the Van Peteghem bonds, which ended up raising nearly €22 billion for the Belgian State.',
            'label':'No',
            'explanation':'The sentence mentions bonds. However, these are sold by the Belgian state, which makes them government bonds, rather than corporate bonds.'
        },
        {
            'sentence_text':"AbbVie's $30 billion: the last hurrah for high-grade corporate dollar bonds?",
            'label':'Yes',
            'explanation':'The words corporate bonds are in the sentence, seperated by dollar. Clearly the sentence discusses corporate bonds.'
        },
        {
            'sentence_text':'Although bond prices may vary, they are often constrained in how high they can rise.',
            'label':'No',
            'explanation':'Although bonds are certainly mentioned, there is no distinction between government and corporate bonds. Since we are only interested in senteces that discuss Corporate Bonds, this sentence is labelled with "No".'
        },
        {
            'sentence_text':'Shwedo cited the combination of buying in U.S. Treasuries and a tightening of credit spreads - or the difference in interest rates between Treasuries and corporate bonds of the same maturity - that has resulted in lower borrowing costs for companies.',
            'label':'Yes',
            'explanation':'Although the sentence begins by discussing "U.S. Treasuries and a tightening of credit spreads", which are more general and not specifically corporate bonds, the sentence then mentions these explicitly, thus receiving the label "Yes".'
        }
    ]

    new = input_maximised(3, 5, 5, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences)

    for k, v in new.items():
        print(f'{k}: {v}')