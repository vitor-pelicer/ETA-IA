import random
import numpy as np
import gymnasium as gym
from gymnasium import spaces


class Ambiente(gym.Env):
    def __init__(self, volume_inicial=0, relevancia_inicial=0, 
                 delta_volume=4000, delta_relevancia=0.4):
        super().__init__()

        self.volume_inicial = volume_inicial
        self.relevancia_inicial = relevancia_inicial

        self.delta_volume_inicial = delta_volume
        self.delta_relevancia_inicial = delta_relevancia

        self.ativacoes = []
        self.media_ativacoes = 0
        
        # Define o espaço de ações:
        self.action_space = spaces.Box(
            low=np.array([-50, -0.1]),  # Limites mínimos para delta_volume e delta_relevancia
            high=np.array([50, 0.1]),   # Limites máximos
            dtype=np.float32
        )

        self.observation_space = spaces.Dict({
            'volume': spaces.Discrete(10001),  # 0 a 10000
            'relevancia': spaces.Discrete(100),  # 0 a 99 -> 0.00 a 0.99
            'delta_volume': spaces.Discrete(10001),  # 0 a 10000
            'delta_relevancia': spaces.Discrete(100),  # 0 a 99 -> 0.00 a 0.99
            'media_ativacoes': spaces.Discrete(201),  # 0 a 200 -> -1.0 a 1.0
        })


        self.reset()

    def reset(self, seed=None, options=None):
        """Reinicia o ambiente para um novo episódio."""
        super().reset(seed=seed) # Para garantir compatibilidade com versões futuras do Gymnasium

        self.volume_atual = self.volume_inicial  
        self.relevancia_atual = self.relevancia_inicial 

        self.delta_volume = self.delta_volume_inicial
        self.delta_relevancia = self.delta_relevancia_inicial

        self.ativacoes = []
        self.media_ativacoes = 0

        # Retorna a observação e informações adicionais (vazio neste caso)
        return self._get_obs(), {}

    def _get_obs(self):
        """Retorna o estado atual do ambiente."""
        return {'volume': int(self.volume_atual), 
                'relevancia': int(self.relevancia_atual * 100),
                'delta_volume': int(self.delta_volume),
                'delta_relevancia': int(self.delta_relevancia * 100),
                'media_ativacoes': int(self.media_ativacoes * 100)+100
                }

    def step(self, action):
        """Aplica a ação e atualiza o ambiente."""
        delta_volume, delta_relevancia = action[0], action[1]  # Extrai os valores do array

        # Atualiza os deltas (sem necessidade de ajustes adicionais)
        self.delta_relevancia += delta_relevancia
        self.delta_volume += delta_volume

        self.delta_relevancia = np.clip(self.delta_relevancia, 0.1, 0.99)
        self.delta_volume = np.clip(self.delta_volume, 200, 10000)

        self.delta_relevancia = np.clip(self.delta_relevancia, 0.1, 0.99)
        self.delta_volume = np.clip(self.delta_volume, 200, 10000)

        # Atualiza o estado (ajuste a lógica conforme necessário)
        self.volume_atual += random.randint(500, 1500) 
        self.relevancia_atual += random.uniform(0.05, 0.15)

        # Impede que os valores ultrapassem os limites
        self.volume_atual = np.clip(self.volume_atual, 0, 10000)
        self.relevancia_atual = np.clip(self.relevancia_atual, 0, 1)

        # Determina se o processo foi ativado
        processo_ativado = self.volume_atual >= self.delta_volume or \
                           self.relevancia_atual >= self.delta_relevancia
        
        # Reset caso o processo seja ativado
        if processo_ativado:
            print(f"Ativou: delta V: {self.delta_volume} delta R: {self.delta_relevancia}")

            if self.volume_atual >= self.delta_volume and self.relevancia_atual >= self.delta_relevancia:
                self.ativacoes.append(0)
            elif self.volume_atual >= self.delta_volume:
                self.ativacoes.append(1)
            elif self.relevancia_atual >= self.delta_relevancia:
                self.ativacoes.append(-1)
            

            if len(self.ativacoes) > 10:
                self.ativacoes.pop(0)

            if len(self.ativacoes) > 0:
                self.media_ativacoes = sum(self.ativacoes) / (len(self.ativacoes))

            print((self.media_ativacoes * 100) + 100)

            self.volume_atual = 0
            self.relevancia_atual = 0

        # Calcula a recompensa
        reward = self.calcular_recompensa(processo_ativado, 
                                          self.volume_atual, 
                                          self.relevancia_atual,
                                          self.delta_volume,
                                          self.delta_relevancia,
                                          self.media_ativacoes
                                          )

        done = False
        truncated = False  

        return self._get_obs(), reward, done, truncated, {}

    def calcular_recompensa(self, processo_ativado, volume, relevancia, delta_volume, delta_relevancia, media_ativacoes):
        recompensa = 0

        # Penalidades por deltas fora do intervalo
        if delta_volume < 2000:
            recompensa -= (2000 - delta_volume) / 1000
        elif delta_volume > 8000:
            recompensa -= (delta_volume - 8000) / 1000

        if delta_relevancia < 0.2:
            recompensa -= (0.2 - delta_relevancia) * 10 
        elif delta_relevancia > 0.8:
            recompensa -= (delta_relevancia - 0.8) * 10

        if abs(media_ativacoes) > 0.5:
            recompensa -= 0.8
        else:
            recompensa += 1

        # Bônus por deltas no intervalo ideal
        if 2000 <= delta_volume <= 8000 and 0.2 <= delta_relevancia <= 0.8:
            recompensa += 1 

        return recompensa
        