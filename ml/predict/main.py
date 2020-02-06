import torch
import pickle
import sys
import os
from tqdm import tqdm

sys.path.append(os.pardir)
import model as m
from dataset import NewestOMOP, RESULT_DB, OMOP_DB

NUMBER_OF_REASONS = 10

if __name__ == '__main__':

    # load pretrained data
    model_path = sys.argv[1]
    with open(model_path, 'rb') as file:
        model_data = pickle.load(file)
    disease_lut = model_data['disease_lut']
    state_dict = model_data['state_dict']
    threshold = model_data['threshold']

    # select device to run on [cpu, cuda]
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    # create database connection
    result_db = RESULT_DB()
    omop_db = OMOP_DB()

    # create dataset
    omop_ds = NewestOMOP(disease_lut)

    # load model
    model = m.SVM(len(omop_ds[0][1]))
    model.load_state_dict(state_dict)
    model.to(device)

    # prepare clinical finding name lut
    ids_d = {omop_ds.disease_lut[i]['cid']: i for i in omop_ds.disease_lut}
    name_lut = [omop_ds.disease_lut[ids_d[cid]]['name'] for cid in sorted(ids_d, key=lambda x: x)]
    vocab_lut = [omop_ds.disease_lut[ids_d[cid]]['vocabulary'] for cid in sorted(ids_d, key=lambda x: x)]
    domain_lut = [omop_ds.disease_lut[ids_d[cid]]['domain'] for cid in sorted(ids_d, key=lambda x: x)]

    # fix model parameters (required for back propagation of feature importance)
    for param in model.parameters():
        param.requires_grad = False

    # generate prediction for every patient
    for patient in tqdm(omop_ds, desc="calculate decubitus risk"):

        # prepare data
        pid, features = patient
        features = features.to(device)
        features.requires_grad = True

        # calculate prediction
        output = model(features.view(1, -1))
        prediction = output[0, 1].item() > threshold

        # calculagte importance of each input feature
        output.backward(torch.tensor([[-1.0, 1.0]]))
        importance = features.grad.cpu().numpy() * (features != 0).float().numpy()
        importance_d = {i: v for i, v in enumerate(importance)}

        # select most important reasons
        top = sorted(importance_d, key=lambda x: abs(importance_d[x]), reverse=True)[:NUMBER_OF_REASONS]

        # get additional patient data
        p_data = omop_db.get_patient_data(pid)

        # clear old data from database
        result_db(f"DELETE FROM results.patient  WHERE patient_id='{pid}' RETURNING ''")
        result_db(f"DELETE FROM results.reasons  WHERE patient_id='{pid}' RETURNING ''")

        # store results in database
        result_db(f"""INSERT INTO results.patient (patient_id, prediction, gender, birthday, zip, city) 
                      VALUES ('{pid}', '{prediction}', '{p_data["gender"]}', '{p_data["birthday"]}', '{p_data["zip"]}', '{p_data["city"]}')
                      RETURNING '' """)

        for reason_id in top:
            reason_text = name_lut[reason_id].replace("'", "`")  # get name of clinical finding and escape single quotes
            reason_value = importance_d[reason_id]


            if domain_lut[reason_id] == "Measurement":
                reason = f'"{reason_text}" should be {"lower" if reason_value < 0 else "higher"}'

            elif domain_lut[reason_id] == "Procedure":
                if reason_value >= 0:
                    # skip Procedure that "should be done" to reduce the risk
                    continue
                reason = f'"{reason_text}" {"was done" if reason_value < 0 else "should be done"}'

            elif domain_lut[reason_id] in ["Condition", "Observation"]:
                if reason_value >= 0:
                    # skip conditions/observations that "should be diagnosed" to reduce the risk
                    continue
                reason = f'{reason_text}'

            elif domain_lut[reason_id] == "proprietary":
                reason = f'{reason_text}'

            else:
                reason = f'{reason_text}'

            result_db(f"""INSERT INTO results.reasons (patient_id, reason) 
                          VALUES ('{pid}', '{reason}')
                          RETURNING '' """)

        # make patients results persistent
        result_db.commit()
