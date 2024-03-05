from openai import OpenAI
import os
import json
from dotenv import load_dotenv

def call_gpt(gpt_key, system_content, assistant_content, user_content, custom_output_function, max_n_tries):
    for i in range(max_n_tries):
        try:
            client = OpenAI(
            api_key=gpt_key,
            )
            model = 'gpt-4'

            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_content},
                    {"role": "assistant", "content": assistant_content},
                    {"role": "user", "content": user_content},
                ],
                tools = custom_output_function,
                tool_choice = 'auto'
            )
            response = json.loads(response.model_dump()["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
            return response
        except Exception as e:
            if i == max_n_tries - 1:
                print(e)
                quit()

def write_topic_information(n, sentences, topic_name, topic_definition, keywords, name_variations, difficult_cases):
    topic_information = f'You are tasked with labelling the following topic:\n\n{topic_name}\n{topic_definition}'

    topic_information += '\n\nHere are some keywords that are often associated with the topic. Note, the mere presence of a keyword in a sentence does not guarantee the presence of the topic.'
    for keyword in keywords:
        topic_information += f'\n{keyword}'

    topic_information += '\n\nHere are some name variations for the topic: other ways in which it is commonly referred to.'
    for name_variation in name_variations:
        topic_information += f'\n{name_variation}'

    topic_information += '\n\nHere are some descriptions of difficult cases for the topic. These are cases which, for whatever reason, might confuse the labeller and lead them to place an incorrect or inaccurate label.'
    for difficult_case in difficult_cases:
        topic_information += f'\n{difficult_case}'

    topic_information += f'\n\nHere is the dictionary with sentences. Your task is to choose {n} sentences from this dictionary, returning their text.'
    for sentence in sentences:
        topic_information += f'\n{sentence}'
    return topic_information

def generate_sentence_selection_prompt(n, sentences, topic_name, topic_definition, keywords, name_variations, difficult_cases):
    introduction_content = f'You are an assistant in a Natural Language Processing, Named Entity Recogniition model-training environment. In particular, you are adept at selecting the most suitable training sentences, which will, using the smallest possible number of sentences, provide the largest possible variation in the data, the largest possible context. You will be selecting sentences for the following topic:' 
    topic_information = write_topic_information(n, sentences, topic_name, topic_definition, keywords, name_variations, difficult_cases)
    instruction_content = f'Consider the sentences in the input dictionary, and return a list with {n} of these sentences according to the utility they provide. The output should be a dictionary with only the sentence_text of the chosen {n} sentences. When preparing this dictionary, make sure that you select the most important sentences. Make sure that you consider the interplay between the sentences: if two sentences say the same thing, then including both at in the list makes little sense; if one sentence is rather unique, maybe this provides a lot of variation to the data, which might make it more important. Make sure to consider the topic context provided above: a sentence is only relevant with respect to how it contributes to the unerstanding of the context of the topic. Notice that the input sentences are labelled with both "No" and "Yes", it is likely a good idea to include a balanced mixture of these labels in the sentences you select. Please take your time, and respect these instructions.'
    system_content = 'You are a training-sample optimisiation expert, proficient at selecting the most efficient training set for a Named Entity Recognition model.'
    assistant_content = ''
    user_content = introduction_content + topic_information + '\n\n' + instruction_content
    return system_content, assistant_content, user_content

def generate_sentence_selection_custom_function(n):
    properties = {}
    for i in range(1, n + 1):
        properties[f'sentence_text_{i}'] = {
            'type': 'string',
            'description': 'A sentence that you would have a difficult time labelling with respect to the given topic.'
        }

    required = []
    for i in range(1,n+1):
        required.append(f'sentence_text_{i}')

    custom_output_function = [
        {
            'type': 'function',
            'function': {
                'name': 'sentence_generation',
                'description': 'Generate sentences for a given topic that would be difficult to label for.',
                'parameters': {
                    'type': 'object',
                    'properties': properties,
                    'required': required
                }
            }
        }
    ]

    return custom_output_function

def select_n_sentences(gpt_key, n, sentences, topic_name, topic_definition, keywords, name_variations, difficult_cases, max_n_tries=5):
    system_content, assistant_content, user_content = generate_sentence_selection_prompt(n, sentences, topic_name, topic_definition, keywords, name_variations, difficult_cases)
    custom_output_function = generate_sentence_selection_custom_function(n)
    response = call_gpt(gpt_key, system_content, assistant_content, user_content, custom_output_function, max_n_tries)
    selected_sentences = [i[1] for i in response.items()]
    l = []
    for s in sentences:
        if s['sentence_text'] in selected_sentences:
            l.append(s)
    return l

if __name__ == "__main__":
    topic_name = 'Corporate Bonds'
    topic_definition = "Corporate bonds are fixed-income investment securities representing ownership of debt, where an investor loans money to a company for a set period of time and receives regular interest payments, providing a means for diversifying portfolios and mitigating investment risk for the investor, and a safe way for the bond issuer, who returns the investor's money once the bond reaches maturity, to access capital."
    keywords = ['Bonds', 'Private Debt', 'Issue', 'Financing', 'Borrow', 'Coupon', 'Maturity', 'Interest']
    name_variations = ['Corporate Debt Security', 'Private Bond', 'Firm Bond']
    difficult_cases = ['Corporate Bonds and Government Bonds are not the same. Distinguishing these can be challenging.', 'Instead of using the word "Bonds" explicitly, these may be referred to as "Debt Instruments" or similar, which might not actually be "Bonds".']

    sentences = [
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
        },
        {
            "sentence_text": "The corporate debt securities are hitting an all time low in the market.",
            "label": "Yes",
            "explanation": "While 'corporate debt securities' does not directly say 'corporate bonds', it is a synonym used for bonds which a company finances. So, in spite of not mentioning 'bonds', the sentence speaks about 'corporate bonds' making it challenging to accurately label."
        },
        {
            "sentence_text": "Johnson & Johnson issued new financing today for expansion purposes.",
            "label": "No",
            "explanation": "The sentence talks about a company issue and financing but doesn't mention whether it is in the form of a bond. This makes it difficult to legitimately identify the occurrence of 'corporate bonds' topic in the statement."
        },
        {
            "sentence_text": "Despite the uncertainty in the economic surroundings, the yield on the firm's private debt didn't waver.",
            "label": "Yes",
            "explanation": "Here, 'private debt' is a way to say 'bonds'. However, since it does not explicitly state 'bonds', and instead uses a terminology often associated with but not exclusive to bonds, it would make the labelling process challenging."
        },
        {
            "sentence_text": "The borrowing rates remained low following Berkshire Hathaway's recent coupon issue.",
            "label": "No",
            "explanation": "Even though 'borrowing' and 'coupon issue' are terms closely related to the topic of 'corporate bonds', the sentence doesn't expressively indicate 'corporate bonds'. This makes it tricky to label accurately."
        },
        {
            "sentence_text": "The pharmaceutical giant, Pfizer, opted for an IPO over maturity debt to raise capital.",
            "label": "No",
            "explanation": "The sentence mentions 'maturity debt' which could potentially confuse it with 'maturity bonds'. But in the context of the sentence, the company opted for an Initial Public Offering (IPO), and not 'bonds'. This makes it a difficult case in relation to the given topic."
        }
    ]
    n=5
    import random
    random.shuffle(sentences)
    selected_sentences = select_n_sentences(n, sentences, topic_name, topic_definition, keywords, name_variations, difficult_cases)
    for i in selected_sentences:
        print(i)