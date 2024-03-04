from sentence_transformers import SentenceTransformer
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import OneHotEncoder
import numpy as np

def load_embedder():
    embedder = SentenceTransformer('all-mpnet-base-v2')
    return embedder

def create_embeddings(data, only_text):
    embedder = load_embedder()
    if only_text:
        corpus_embeddings = embedder.encode(data)
        corpus_embeddings = corpus_embeddings / np.linalg.norm(corpus_embeddings, axis=1, keepdims=True)
    else:
        sentences = [item['sentence_text'] for item in data]
        explanations = [item['explanation'] for item in data]
        
        sentence_embeddings = embedder.encode(sentences)
        explanation_embeddings = embedder.encode(explanations)
        sentence_embeddings = sentence_embeddings / np.linalg.norm(sentence_embeddings, axis=1, keepdims=True)
        explanation_embeddings = explanation_embeddings / np.linalg.norm(explanation_embeddings, axis=1, keepdims=True)

        labels = [item['label'] for item in data]
        one_hot_encoder = OneHotEncoder(sparse_output=False)
        label_embeddings = one_hot_encoder.fit_transform(np.array(labels).reshape(-1, 1))

        corpus_embeddings = np.concatenate((sentence_embeddings, label_embeddings, explanation_embeddings), axis=1)
    return corpus_embeddings

def cluster_assigning(distance_threshold, corpus_embeddings):
    clustering_model = AgglomerativeClustering(n_clusters=None, distance_threshold=distance_threshold, metric='cosine', linkage='average')
    clustering_model.fit(corpus_embeddings)
    return clustering_model.labels_

def optimise_distance_threshold(corpus_embeddings):
    dts, cs, ss = [], [], []
    for dt in np.linspace(0,2,200):
        cluster_assignment = cluster_assigning(dt, corpus_embeddings)
        if len(list(set(cluster_assignment))) not in [1,len(corpus_embeddings)]:
            dts.append(dt)
            cs.append(len(list(set(cluster_assignment))))
            ss.append(silhouette_score(corpus_embeddings, cluster_assignment, metric='cosine'))
    l = [dts[i] for i in range(len(dts)) if ss[i] == np.max(ss)]
    return np.median(l)

def sentences_clustered(cluster_assignment, sentences):
    clustered_sentences = {cluster_id:[] for cluster_id in cluster_assignment}
    for sentence_id, cluster_id in enumerate(cluster_assignment):
        clustered_sentences[cluster_id].append(sentences[sentence_id])
    return clustered_sentences

def cluster_sentences(sentences, only_text=True):
    corpus_embeddings = create_embeddings(sentences, only_text)

    dt = optimise_distance_threshold(corpus_embeddings)
    cluster_assignment = cluster_assigning(dt, corpus_embeddings)

    return sentences_clustered(cluster_assignment, sentences)

def yes_no_cluster_sentences(sentences):
    yes = []
    no = []
    for sentence in sentences:
        if sentence['label'] == 'Yes':
            yes.append(sentence)
        else:
            no.append(sentence)

    yes_cluster = cluster_sentences(yes, only_text=False)
    no_cluster = cluster_sentences(no, only_text=False)
    return [yes_cluster[k] for k in yes_cluster.keys()] + [no_cluster[k] for k in no_cluster.keys()]

if __name__ == "__main__":
    only_text = False
    if only_text:
        corpus = [
            "A man is eating food.",
            "A man is eating a piece of bread.",
            "A man is eating pasta.",
            "The girl is carrying a baby.",
            "The baby is carried by the woman",
            "A man is riding a horse.",
            "A man is riding a white horse on an enclosed ground.",
            "A monkey is playing drums.",
            "Someone in a gorilla costume is playing a set of drums.",
            "A cheetah is running behind its prey.",
            "A cheetah chases prey on across a field.",
        ]
        d = cluster_sentences(corpus)
        for k in d.keys():
            print(d[k])
    else:
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
        import random
        random.shuffle(sentences)
        clusters = yes_no_cluster_sentences(sentences)
        for c in clusters:
            for s in c:
                print(s)
            print('\n\n')