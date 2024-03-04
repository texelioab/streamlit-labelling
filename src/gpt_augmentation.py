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

def write_topic_information(topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences):
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

    topic_information += '\n\nHere are some sample sentences, with labels attached and an explanation for the "Yes", "Maybe", or "No" label.'
    for labelled_sentence in labelled_sentences:
        topic_information += f'\n{labelled_sentence}'
    return topic_information

def generate_sentences_prompt(n, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences):
    introduction_content = f'Imagine that you are a Named Entity Recognition service that predicts whether a topic is present in a given sentence. You are an expert within financial news, and you identify these topics in sentences taken from financial sources.' 
    topic_information = write_topic_information(topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences)
    instruction_content = f'I want you to provide {n} sentences, in a similar format (a dictionary with three keys: sentence, label, explanation), that you would have a difficult time labelling. That is, what are {n} sentences which, if you were tasked with labelling them, you would be unsure what to do. In the topic description above, there are some examples that could in theory be tricky for you; please read these carefully, try to understand why they might be difficult for you. Provide sentences that would be difficult related to this, as well as any reasons that you can think of. Feel free to liberally make use of the keywords and name variations. Importantly, do not label the sentences that you create with "Maybe": do either "Yes" or "No", and explain why, despite it being a difficult topic, you chose "Yes"/"No". This is important, please take your time and carefully adhere to these instructions. Only return the {n} dictionaries with keys "sentence", "label", "explanation".'
    system_content = 'You are a Sentence Generation expert, creating sentences that would be difficult for a Named Entity Recognition expert. You are self-reflective, being aware of cases that would be difficult for you.'
    assistant_content = ''
    user_content = introduction_content + topic_information + '\n\n' + instruction_content
    return system_content, assistant_content, user_content

def generate_sentences_custom_function(n):
    properties = {}
    for i in range(1, n + 1):
        properties[f'sentence_{i}'] = {
            'type': 'string',
            'description': 'A sentence that you would have a difficult time labelling with respect to the given topic.'
        }
        properties[f'label_{i}'] = {
            'type': 'string',
            'description': '"Yes" or "No", depending on whether the topic is present in this difficult sentence.',
            'enum': ['Yes', 'No']
        }
        properties[f'explanation_{i}'] = {
            'type': 'string',
            'description': 'A short text (one or two sentences) explaining why this is a difficult sentence, and why you label "Yes" or "No". In your argumentation, refer to the relevant context (name variations, labelled sentences, etc) to strengthen your explanation.'
        }

    required = []
    for i in range(1,n+1):
        required.extend([f'sentence_{i}', f'label_{i}', f'explanation_{i}'])

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

def generate_n_sentences(gpt_key, n, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences, max_n_tries=5):
    system_content, assistant_content, user_content = generate_sentences_prompt(n, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences)
    custom_output_function = generate_sentences_custom_function(n)
    response = call_gpt(gpt_key, system_content, assistant_content, user_content, custom_output_function, max_n_tries)
    return [{'sentence_text':response[f'sentence_{i}'], 'label': response[f'label_{i}'], 'explanation':response[f'explanation_{i}']} for i in range(1,n+1)]

def generate_kw_nv_dc_prompt(topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences):
    introduction_content = f'Imagine that you are a Named Entity Recognition service that predicts whether a topic is present in a given sentence. You are an expert within financial news, and you identify these topics in sentences taken from financial sources.' 
    topic_information = write_topic_information(topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences)
    instruction_content = f'The above keywords, name variations and descriptions of difficult cases are by no means exhaustive. I want you to fill out these lists and provide any more that you can think of. Given the definition of the topic and some of the labelled sentences, as well as using the context from the given keywords, name variaitons and difficult cases, provide any others that are not covered or included. Your output should be a dictionary with three keys, "keywords", "name_variations", "difficult_cases", with each key linking to a list of each of these, respectively, which you thought were missing from the provided lists. This is important, please take your time and carefully adhere to these instructions.'
    system_content = 'You are a financial expert, knowledgeable in Named Entity Recognition, proficient at analysing given contexts and identifying features that are especially important or characteristic of it. In particular, for provided financial topics, you are an expert at finding keywords, name variations and difficult cases related to that topic, that could be useful for labelling sentences for that topic.'
    assistant_content = ''
    user_content = introduction_content + topic_information + '\n\n' + instruction_content
    return system_content, assistant_content, user_content

def generate_kw_nv_dc_custom_function():
    custom_output_function = [
        {
            'type': 'function',
            'function': {
                'name': 'keyword_name_variation_difficult_cases_lists',
                'description': 'Generates lists of keywords, name variations, and descriptions of difficult cases.',
                'parameters': {
                    'type': 'object',
                    'properties': {
                        'keywords': {
                            'type': 'array',
                            'items':{
                                'type':'string',
                                'description': 'A keyword which is often associated with the topic.'
                            },
                            'description': 'A list of keywords which are often associated with the topic.'
                        },
                        'name_variations': {
                            'type': 'array',
                            'items': {
                                'type':'string',
                                'description': 'A name variation of the topic: an alternative way in which it is referred to.'
                            },
                            'description': 'A list of name variations of the topic: alternative ways in which it is referred to.'
                        },
                        'difficult_cases': {
                            'type': 'array',
                            'items': {
                                'type': 'string',
                                'description': 'A description of a difficult case for the topic: a context or scenario which would make it difficult to label for the topic.'
                            },
                            'description': 'A list of descriptions of difficult cases for the topic: contexts or scenarios which would make it difficult to label for the topic.'
                        }
                    },
                    'required': ['keywords', 'name_variations', 'difficult_cases']
                }
            }
        }
    ]
    
    return custom_output_function

def generate_kw_nv_dc(gpt_key, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences, max_n_tries = 5):
    system_content, assistant_content, user_content = generate_kw_nv_dc_prompt(topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences)
    custom_output_function = generate_kw_nv_dc_custom_function()
    response = call_gpt(gpt_key, system_content, assistant_content, user_content, custom_output_function, max_n_tries)
    return response

if __name__ == "__main__":

    n = 5

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
    load_dotenv('credentials.env')
    GPT_TOPICS_KEY=os.getenv('GPT_TOPICS_KEY')

    sentences = generate_n_sentences(GPT_TOPICS_KEY, n, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences)
    for i in sentences:
        print(i,'\n')
    '''with open(os.path.realpath(rf'sentences.json'), 'r', encoding='utf-8') as f:
        data = json.load(f)
    sentences.extend(data)'''
    with open(os.path.realpath(rf'sentences.json'), 'w') as f:
        json.dump(sentences, f, indent=4)
    r = generate_kw_nv_dc(GPT_TOPICS_KEY, topic_name, topic_definition, keywords, name_variations, difficult_cases, labelled_sentences)
    print(r)