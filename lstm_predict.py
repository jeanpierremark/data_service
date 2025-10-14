import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential # type: ignore
from tensorflow.keras.layers import LSTM, Dense, Dropout # type: ignore
from tensorflow.keras.optimizers import Adam # type: ignore
import datetime
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# Fonction pour préparer les données
def prepare_data(df, features, sequence_length=30, predict_days=10):
    # Normalisation des données
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df[features])

    # Création des séquences
    X, y = [], []
    for i in range(len(scaled_data) - sequence_length - predict_days):
        X.append(scaled_data[i:i + sequence_length])
        y.append(scaled_data[i + sequence_length:i + sequence_length + predict_days, :])

    X = np.array(X)
    y = np.array(y)

    # Division en ensembles d'entraînement et de test
    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]

    return X_train, y_train, X_test, y_test, scaler

# Création du modèle LSTM
def create_lstm_model(sequence_length, n_features, predict_days):
   
    model = Sequential([
        LSTM(100, return_sequences=True, input_shape=(sequence_length, n_features)),
        Dropout(0.2),
        LSTM(50),
        Dropout(0.2),
        Dense(predict_days * n_features),
    ])
    model.compile(optimizer=Adam(learning_rate=0.001), loss='mse')
    return model

# Fonction pour évaluer le modèle
def evaluate_model(model, X_test, y_test, scaler, features, predict_days):
  
    # Prédictions sur l'ensemble de test
    predictions = model.predict(X_test)
    predictions = predictions.reshape(-1, predict_days, len(features))
    predictions = scaler.inverse_transform(predictions.reshape(-1, len(features))).reshape(-1, predict_days, len(features))
    
    # Dénormalisation des cibles
    y_test = scaler.inverse_transform(y_test.reshape(-1, len(features))).reshape(-1, predict_days, len(features))
    
    # Calcul des métriques par feature
    metrics = {}
    for i, feature in enumerate(features):
        y_true = y_test[:, :, i].flatten()
        y_pred = predictions[:, :, i].flatten()
        
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        mae = mean_absolute_error(y_true, y_pred)
        r2 = r2_score(y_true, y_pred)
        
        metrics[feature] = {
            'RMSE': rmse,
            'MAE': mae,
            'R2': r2
        }
    
    return metrics

# Fonction pour visualiser les prédictions
def plot_predictions(df, predictions, features, sequence_length, predict_days):
  
    plt.figure(figsize=(12, 8))
    for i, feature in enumerate(features):
        plt.subplot(len(features), 1, i+1)
        # Données historiques (derniers 30 jours)
        plt.plot(range(-sequence_length, 0), df[feature][-sequence_length:], label='Historique', color='blue')
        # Prédictions (10 jours)
        plt.plot(range(1, predict_days+1), predictions[:, i], label='Prédiction', color='orange')
        plt.title(f'{feature.capitalize()}')
        plt.xlabel('Jours')
        plt.ylabel(feature)
        plt.legend()
        plt.grid(True)
    plt.tight_layout()
    plt.show()

# Fonction principale
def main():
    # Chargement des données (remplacez par vos données réelles, par exemple un fichier CSV)
    # Exemple fictif avec données aléatoires
    df = pd.DataFrame({
        'temperature': np.random.normal(20, 5, 1000),
        'precipitation': np.random.exponential(2, 1000),
        'humidity': np.random.normal(60, 10, 1000)
    })

    # Paramètres
    features = ['temperature', 'precipitation', 'humidity']
    sequence_length = 30  # 30 jours précédents
    predict_days = 10    # Prédire 10 jours

    # Préparation des données
    X_train, y_train, X_test, y_test, scaler = prepare_data(df, features, sequence_length, predict_days)

    # Création et entraînement du modèle
    model = create_lstm_model(sequence_length, len(features), predict_days)
    model.fit(X_train, y_train.reshape(y_train.shape[0], -1),
              epochs=100, batch_size=32, validation_split=0.1, verbose=1)

    # Évaluation du modèle
    metrics = evaluate_model(model, X_test, y_test, scaler, features, predict_days)
    print("\nMétriques d'évaluation sur l'ensemble de test:")
    for feature, values in metrics.items():
        print(f"\n{feature.capitalize()}:")
        print(f"RMSE: {values['RMSE']:.2f}")
        print(f"MAE: {values['MAE']:.2f}")
        print(f"R²: {values['R2']:.2f} (valeur entre -∞ et 1, où 1 indique une prédiction parfaite)")

    # Prédiction pour les 10 prochains jours
    last_sequence = scaler.transform(df[features][-sequence_length:])
    last_sequence = last_sequence.reshape((1, sequence_length, len(features)))
    prediction = model.predict(last_sequence)
    prediction = prediction.reshape((predict_days, len(features)))
    prediction = scaler.inverse_transform(prediction)

    # Affichage des prédictions
    current_date = datetime.datetime.now()
    print("\nPrévisions météo pour les 10 prochains jours:")
    for i in range(predict_days):
        date = current_date + datetime.timedelta(days=i+1)
        print(f"{date.strftime('%Y-%m-%d')}:")
        print(f"Température: {prediction[i, 0]:.1f}°C")
        print(f"Précipitation: {prediction[i, 1]:.1f} mm")
        print(f"Humidité: {prediction[i, 2]:.1f}%")
        print()

    # Visualisation des prédictions
    plot_predictions(df, prediction, features, sequence_length, predict_days)

if __name__ == "__main__":
    main()